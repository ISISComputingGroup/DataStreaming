## Greenfield development, ISIS-specific filewriter

### Background

This filewriter option proposes a brand-new development, specific to ISIS.

This would be implemented in Rust, as per other performance-sensitive components of the datastreaming pipeline.

Unlike the SuperMuSR and ESS options, every requirement below is unimplemented. Therefore, this document focuses on *how* we would propose to implement each requirement in a greenfield system.

### Fit with requirements

The overall target architecture is *similar* to the architecture of the ESS filewriter, but explicitly allowing *multiple* writer modules to each subscribe to the same input Kafka messages; i.e. rather than having an `ev44_writer`, we would have an `event_data` writer, which happens to subscribe to `ev44` and/or other message types as required.

Architecturally:
- Configurable nexus_template, defined in runStart message, comparable to ESS filewriter.
- Writer modules for one Nexus dataset or group of related datasets (e.g. `NXevent_data` writer, `NXlog` writer, `isis_vms_compat` writer). Each writer module has complete, exclusive, ownership of one dataset - multiple writer modules will not update the same dataset.
- Each writer module can register to be notified of a specified set of message schemas, on one or more topics.
- When a Kafka message arrives, it is deserialised once centrally, and then every writer module that has registered for that schema + topic combination is passed a reference to that (typed) message
  * This will happen sequentially, in a fixed order. i.e. Message N will be delivered to Writer1, Writer2, Writer3, before message N+1 is delivered to Writer1, Writer2, Writer3. This is to reduce error-recovery and consistency hazards where two writers see messages in a different order than each other.
  * Where messages are on the same topic, message offset N will be received before message N+1. Where messages are on different Kafka topics or partitions, the order is undefined but consistent between any two writer modules.
- Writer modules may be stateful, for example if they need to accumulate data into a histogram or running counter, or if they need to keep track of the period-number for the current frame.

#### Core infrastructure

We will need to:
- Implement a writer-module framework with some concept of writer-modules subscribing to a writer-defined set of `(topic, schema)` combinations (the topics may only be known after the `nexus_structure` is parsed, so this needs to be dynamic).
- Define how our `nexus_structure` will look - wherever possible, following the ESS approach, but minor modifications may be required.
- Implement logic to parse the provided `nexus_structure`, write out static data immediately, and dynamically create the relevant writer-modules.
- Implement a Kafka-listening and message-routing layer, which subscribes to the set of topics needed by any writer module, decodes messages, and routes `(topic, schema)` combinations to the writer-modules interested in those messages.
- Implement a (minimal) framework or abstraction over the HDF5 libraries to ease writer-module implementation.
- Implement logic supporting the run start / run stop / intermediate file lifecycle, including status reporting via `x5f2` and writing-done notifications via `wrdn`
- Logging, metrics, and other required supportability machinery

#### NeXus file format

- [Neutron event data](https://github.com/isisComputingGroup/datastreaming/issues/83)
  * Implemented by a custom writer-module listening to `ev44` and `pu00` from `_events` topic. Localised to a single writer module.
- [Neutron histogram data](https://github.com/isisComputingGroup/datastreaming/issues/84)
  * Implemented by a custom writer-module listening to `ev44` and `pu00` from `_events` topic, and `vc00` from `_vetoConfig` topic. Localised to a single writer module.
  * Implement this **inside the filewriter**, rather than as a separate post-processing step, because we are likely to need all the permutations of:
    - Event mode only
    - Histogram mode only
    - Both events and histograms, describing the same data, in the same file
    - Separate event and histogram files describing the same data
    - Detectors in event mode but monitors histogrammed
- [Static metadata](https://github.com/isisComputingGroup/datastreaming/issues/87)
  * Statically inserted into `nexus_structure`; this is trivial work per-dataset, once the core infrastructure work is complete.
- [Dynamic metadata](https://github.com/isisComputingGroup/datastreaming/issues/88)
  * Implemented by custom writer-modules that implement the specific accumulation logic for that dataset or group. Localised to a number of new writer modules, most of which are not complicated, but there are a number of them to implement.
- [SELog / blocks](https://github.com/isisComputingGroup/datastreaming/issues/89) - ensure to include the 'going back in time' requirement
  * Implemented by a writer-module, similar to writer-module in ESS filewriter which covers back-in-time requirement (alongside forwarder emitting updates for all PVs at predictable intervals even if unchanged)
  * Subscribes to alarm, unit, value messages from `_sampleEnv` topic, going 'back in time' by at least the filewriter's periodic-forwarding interval at run start.
- [EPICS PutLog](https://github.com/isisComputingGroup/datastreaming/issues/91)
  * Would use either a new generic writer-module for `NXtextlog`, or the generic `NXlog` writer-module as above.
  * Needs extra infrastructure to forward the `PutLog` to Kafka in the first place - likely using `vs00` schema.
- [`runlog/icp_event`](https://github.com/isisComputingGroup/datastreaming/issues/92)
  * Specialised writer-module; needs to subscribe to a number of different topics in order to fabricate an `icp_event` dataset that resembles what the ICP previously wrote (and what Mantid therefore expects).
- [Period-specific metadata](https://github.com/isisComputingGroup/datastreaming/issues/93)
  * Specialised writer-module, subscribing to `ev44`, `pu00`, and any other necessary schemas
- [`isis_vms_compat`](https://github.com/isisComputingGroup/datastreaming/issues/94)
  * Static metadata in `nexus_structure` wherever possible; these datasets are then very cheap to add support for.
  * Specialised writer module(s) where runtime calculations are required.
- [Monitor data](https://github.com/isisComputingGroup/datastreaming/issues/95)
  * Re-uses bulk of the support/logic from either event-data or histogram data as appropriate.
- [`instrument` group](https://github.com/isisComputingGroup/datastreaming/issues/97)
  * Static metadata in `nexus_structure` wherever possible; these datasets are then very cheap to add support for.
  * Specialised writer module(s) where runtime calculations are required.
- [Instrument-specific nexus extras e.g. `instrument_components.nxs`](https://github.com/ISISComputingGroup/DataStreaming/issues/106)
  * Static metadata in `nexus_structure` wherever possible; these datasets are then very cheap to add support for.

#### Downstream workflows

- [File archiving workflows](https://github.com/ISISComputingGroup/DataStreaming/issues/85)
- [Generating journals](https://github.com/isisComputingGroup/datastreaming/issues/86)

The filewriter would emit a `wrdn` message when it has completed writing a file, containing both the run-start metadata (e.g. run number) and metadata computed during the run (e.g. accumulated proton charge).

The metadata computed during the run would be designed in a comparable way to the writer-modules: it 'subscribes' to messages that it wants to be notified of during a run, and accumulates those while the run
is in progress. This ensures that the emitted `wrdn` saw the exact same messages as the filewriter and describes the same data - including in the case of dropped messages or other bad-data scenarios.

This `wrdn` message would be used to trigger journaling and archiving processes, which would live in separate processes outside the filewriter.

#### [Intermediate and autosave files](https://github.com/isisComputingGroup/datastreaming/issues/96)

We would add a Kafka message which commands the filewriter to write an intermediate file 'now'. At the next self-consistent time, the filewriter would:
- Close the in-progress file
- 'fork' the in-progress file (by copying bytes on disk)
- Perform any end-of-run actions and save out one of the files as the 'intermediate'.
- Re-open and continue accumulating any new messages into the original file.

Because this filewriter operates in a pooled configuration, a fallback, reducing complexity but also potentially reducing performance, would be to emit parallel run start and run stop messages, picked up by another filewriter process to generate intermediate files. The separate filewriter process would then have to re-ingest all relevant messages from Kafka in order to write it's file. This is a low-risk option, and may be suitable for HRPD-X where data rates are not particularly high.

#### [Deployment topology](https://github.com/isisComputingGroup/datastreaming/issues/98)

We would adopt a pooled architecture, staying close to the ESS design: worker selection by all filewriters joining a common Kafka consumer-group for the filewriter command topic.

As per both ESS and SuperMuSR filewriters, we would attempt to avoid adding any dependency on a specific deployment technology in the filewriter repository itself (with the exception of basic, optional, container images).

Graceful shutdown logic comparable to the ESS filewriter's SIGHUP mechanism would allow orchestrators to gracefully perform rolling restarts/upgrades, while not tying us tightly to a specific technology.

### Code quality

- No code currently exists, so there is nothing to evaluate.
- Our testing strategy should take inspiration from the ESS filewriter, which has test coverage at multiple levels.
- We should also aim to have a system test which compares our output against an ICP-generated file, and asserts that the layout is identical other than known, intentional differences.

### Supportability

- Logging would be performed via standard rust logging infrastructure
- Metrics would be exposed via prometheus-scrape compatible endpoints, comparable to other streaming processes. This allows metrics to be consumed easily and concurrently by our existing Nagios monitoring infrastructure, DSG Grafana dashboards, and any future monitoring infrastructure chosen by the MNeuData project.
- Error handling and error recovery schemes will need to be defined as part of implementation, but should take note of the failure modes already accounted for in either the SuperMuSR or ESS filewriters.
