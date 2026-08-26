## ESS Filewriter

### Background

The [ESS filewriter](https://github.com/ess-dmsc/kafka-to-nexus) is the ESS' filewriter process.

### Fit with requirements

#### NeXus file format

In general: the biggest architectural hurdle is that ESS writer modules expect, for the most part, a one-to-one mapping between input schemas and writer modules.

We will have many different nexus datasets which would need to depend on the data contained in a single message. The analysis below *assumes* that the ESS writer-module framework can allow for both:
- A single message to be delivered to multiple writer modules. This appears to already be true, but not the 'usual' case.
- A single writer module to be able to subscribe to multiple message types. This appears to be the 'harder' refactor, and would need architectural attention.
Without these assumptions, many of the arguments below that depend on new writer modules being written collapse.

- [Neutron event data](https://github.com/isisComputingGroup/datastreaming/issues/83)
  * **Generally supported**
  * To fully support this, we would need to add support for ISIS-specific quirks of our Nexus files. This is expected to be a relatively modest, localised task - not a rewrite.
- [Neutron histogram data](https://github.com/isisComputingGroup/datastreaming/issues/84)
  * **Unsupported**
  * To support this, we would need to:
    * Add a new writer module for a histogrammed representation of events.
    * Ensure that events can be written to both a histogrammed representation and an event representation if both modules are present in the `nexus_structure`.
- [Static metadata](https://github.com/isisComputingGroup/datastreaming/issues/87)
  * **Mostly supported**
    * The vast majority of the listed datasets could be included in the `nexus_structure` JSON, with no code changes required.
    * The `mdat` writer module has specific support for start and end time.
    * The only requirement that is potentially unclear is metadata which can update *during* a run, and should have only its most recent value written to the file (for example run title).
  * To fully support this, we may need to add a new writer module which updates a single value in-place (rather than appending new values to a log). This is a relatively minor change, localised to a single new writer module.
- [Dynamic metadata](https://github.com/isisComputingGroup/datastreaming/issues/88)
  * **Unsupported**
    * There is no obvious support for the concept of 'dynamic' metadata in the ESS filewriter, except for a very limited `mdat` writer which only handles start/stop times.
  * To fully support this, we would need to:
    * Add new writer module(s), which *compute* or *accumulate* data, before writing them to the file. There is limited precedent for this in the `mdat` writer. We would likely need a new writer module for each type of 'dynamic' metadata.
- [SELog / blocks](https://github.com/isisComputingGroup/datastreaming/issues/89) - ensure to include the 'going back in time' requirement
  * **Mostly supported**
    * Support for most datasets of interest is present.
    * Units are [statically supported](https://github.com/ess-dmsc/kafka-to-nexus/blob/232c7173d2e15bebcc4365accd6c32d616148eb9/src/WriterModule/f144/f144_Writer.h#L84); we would need to add support for `un00` unit updates via Kafka.
    * The ESS filewriter has [a configurable 'back-in-time' parameter](https://github.com/ess-dmsc/kafka-to-nexus/blob/232c7173d2e15bebcc4365accd6c32d616148eb9/apps/kafka-to-nexus.cpp#L257) and [has logic to buffer `f144` updates](https://github.com/ess-dmsc/kafka-to-nexus/blob/232c7173d2e15bebcc4365accd6c32d616148eb9/src/Stream/SourceFilter.h#L35).
  * To fully support this, we would need to add support for units from Kafka `_sampleEnv` topic, as opposed to from the run start. This should be a relatively small change, localised to the `f144` writer-module.
- [EPICS PutLog](https://github.com/isisComputingGroup/datastreaming/issues/91)
  * **Unsupported**
    * String logs are not supported; the reasoning was that storing strings in `NXlog` is not nexus-compliant. `NXtextlog` is now available for this purpose.
    * ESS schemas already include a `vs00` schema suitable for emitting string-typed logs through the Kafka layer.
  * To support this, we would need to implement a writer module for the existing `vs00` ESS schema to `NXtextlog`. This would follow broadly the same shape as the existing `f144` writer module.
- [`runlog/icp_event`](https://github.com/isisComputingGroup/datastreaming/issues/92)
  * **Unsupported**
    * There isn't currently a concept of internally-generated logs in the ESS filewriter - especially not ones which depend on multiple input streams.
  * We would need to carefully assess how best to fit this into the ESS filewriter architecture. There is no clear precedent.
- [Period-specific metadata](https://github.com/isisComputingGroup/datastreaming/issues/93)
  * **Partially supported** today, **Supported** alongside "dynamic metadata" work
    * Some datasets can be statically known at runstart: these are trivial to add as part of the nexus structure.
    * Datasets which depend on the event data inherit the same problem as the "dynamic metadata" section.
  * To support this, we would use the existing `nexus_structure` support for any static metadata, and implement "dynamic metadata" writer-modules for any dynamic elements.
- [`isis_vms_compat`](https://github.com/isisComputingGroup/datastreaming/issues/94)
  * **Partially supported** today, **Supported** alongside "dynamic metadata" work
    * Some datasets can be statically known at runstart: these are trivial to add as part of the nexus structure.
    * Datasets which depend on the event data inherit the same problem as the "dynamic metadata" section.
  * To support this, we would use the existing `nexus_structure` support for any static metadata, and implement "dynamic metadata" writer-modules for any dynamic elements.
- [Monitor data](https://github.com/isisComputingGroup/datastreaming/issues/95)
  * **Partially supported** today, **Supported** alongside histogram-generation work
    * Monitor events should be trivially supportable, via nexus_structure defining appropriate locations for `ev44` writer modules.
    * Generating nexus histograms from Kafka events inherit the same problems as 'neutron histogram data'
    * Monitor histograms `hs00`/`hs01` don't appear to be supported (are ESS doing all monitors in event mode?)
  * To support this, we would need to:
    * Do the 'histogram writer module' work
    * If we need streamed-histogram support (`hs00`/`hs01`) later, implement a new writer-module for these. This can likely be deferred for HRPD-X.
- [`instrument` group](https://github.com/isisComputingGroup/datastreaming/issues/97)
  * **Supported** - via `nexus_structure`.
- [Instrument-specific nexus extras e.g. `instrument_components.nxs`](https://github.com/ISISComputingGroup/DataStreaming/issues/106)
  * **Supported** - via `nexus_structure`.

#### Downstream workflows

The ESS filewriter emits a `wrdn` on completion of a file, and this should be suitable as a 'hook' on which to attach workflows such as file-archiving and journal-writing, implemented in separate processes.

One area that would need attention is that the journal wants to write fields that *depend on the content of the file*, for example total counts or good uAh. At present, the metadata emitted in the `wrdn` is a copy of the metadata from the runStart. This means that consumers would either have to inspect the just-written file to pull out this data, or the `wrdn` machinery could be modified to (optionally) include some of the fields derived from the event data during the run.

Files [are marked as read-only on close](https://github.com/ess-dmsc/kafka-to-nexus/blob/232c7173d2e15bebcc4365accd6c32d616148eb9/src/HDFFile.cpp#L41).

Therefore for our requirements:

- [File archiving workflows](https://github.com/ISISComputingGroup/DataStreaming/issues/85)
  * **Supported**, in the sense that appropriate messages are emited which could be picked up by an external archiving process.
- [Generating journals](https://github.com/isisComputingGroup/datastreaming/issues/86)
  * **Partially supported**; we would need to extend the content of the `wrdn` message to do this optimally.

#### [Intermediate and autosave files](https://github.com/isisComputingGroup/datastreaming/issues/96)

- **Unsupported**; it is unclear how this would be implemented. 
  - The ESS filewriter *does* use HDF5 SWMR support, so a separate/cooperating process may be able to take a 'snapshot' of an in-progress file, fix-up the metadata, and then emit that as an intermediate file.
  - We could emit a new runStart/runStop pair, which would be picked up by another filewriter in the job-pool. This is an option because the ESS filewriter is inherently pooled and parallelizable by adding more writer processes. However, this would cause an expensive re-read of all of the data in the run from Kafka, and may cause our intermediate files to then be emitted so slowly that they are not useful to scientists.
  - We could add full 'snapshotting' support to the filewriter, via a dedicated Kafka message, but this may be a rather invasive change.

#### [Deployment topology](https://github.com/isisComputingGroup/datastreaming/issues/98)

The ESS filewriter is deployed in a pooled architecture: a set of independent filewriter processes, each one capable of picking up a writing job from any instrument. Worker selection is by filewriters subscribing using the same Kafka consumer group while idle. Workers may freely join the pool. `SIGHUP` is used to notify a filewriter to complete it's current task and then shut down.

Deployment infrastructure is not in the repository (beyond a minimal Dockerfile), and the filewriter makes no assumptions about deployment technology (beyond a Linux OS).

### Code quality

- Major dependencies are listed at https://github.com/ess-dmsc/kafka-to-nexus/tree/232c7173d2e15bebcc4365accd6c32d616148eb9#building-the-applications : Linux, Conan, CMake, C++17, Ninja
- A large number of [unit tests](https://github.com/ess-dmsc/kafka-to-nexus/tree/232c7173d2e15bebcc4365accd6c32d616148eb9/tests), [integration tests](https://github.com/ess-dmsc/kafka-to-nexus/tree/232c7173d2e15bebcc4365accd6c32d616148eb9/integration-tests) and [domain tests](https://github.com/ess-dmsc/kafka-to-nexus/tree/232c7173d2e15bebcc4365accd6c32d616148eb9/domain-tests) are included in the repository.
- The codebase is C++17.
  * This deviates from our usual convention of using Rust for performance-sensitive processes and Python otherwise elsewhere in the streaming stack.
  * We will need to accept the higher maintenance cost of a C++ codebase compared to other languages, if we select this option.
  * Some of the correctness risks of using C++ are mitigated by the good test coverage and [routine use of sanitizers](https://github.com/ess-dmsc/kafka-to-nexus/blob/main/conanfile.py#L34)
- The codebase is in a standalone repository that is reasonably well decoupled from other ESS-specific infrastructure.
- Historic collaboration with ESS has been somewhat patchy - we may struggle to reliably upstream all changes we want to make in a timely manner for HRPD-X, given our relatively tight implementation deadline. Therefore, a local fork, upstreaming changes *when possible*, seems like the most likely option. This creates the risk of significant later divergence unless we make a strong attempt to reconcile after HRPD-X implementation.
  * [`CONTRIBUTING.MD` suggests that all changes would need to go through an ESS steering board](https://github.com/ess-dmsc/kafka-to-nexus/blob/main/CONTRIBUTING.md#the-project); it is highly unlikely that this would be a viable approach in time for HRPD-X commissioning, which strengthens the argument that we would be developing a local fork rather than upstream-first.
- Some documentation exists, but correctness is patchy: for example [the `f144` documentation actually describes an `NXevent_data` structure](https://github.com/ess-dmsc/kafka-to-nexus/blob/232c7173d2e15bebcc4365accd6c32d616148eb9/documentation/writer_module_f144_logdata.md?plain=1#L42).

### Supportability

- Reasonable logging is present.
- The most important metrics are present.
- Periodic status reports via an `x5f2` schema are emitted.
- [Invalid messages are filtered](https://github.com/ess-dmsc/kafka-to-nexus/blob/main/src/Stream/SourceFilter.cpp) and published to metrics - including corrupt messages and messages with too-old timestamps
- Some invalid data is detected, but then written anyway. For example, `ev44` messages with mismatched `pixel_id` and `time_of_flight` array lengths are [logged but *then written anyway*](https://github.com/ess-dmsc/kafka-to-nexus/blob/main/src/WriterModule/ev44/ev44_Writer.cpp#L103), which would cause data corruption. 
  * We would need to carefully review other writer modules for these kinds of failure paths, and would likely want to drop these messages (with logs/metrics) to avoid corrupting the entire file.
- It is not clear how the filewriter recovers if it exits non-gracefully during writing (e.g. segfault, forceful shutdown, server power-off); in principle this should be *detectable* from an external process due to a lack of `x5f2` status reports.
  * We may need some external 'watchdog' service which detects file-writing tasks which failed non-gracefully, and re-queues those writing jobs with a limited number of retries. It would also need to 'clean up' incomplete partially-written files from a non-graceful termination. Not immediately clear whether ESS already have such a service.
