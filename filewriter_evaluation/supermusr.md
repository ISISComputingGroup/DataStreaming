## SuperMuSR filewriter

### Background

The [SuperMuSR filewriter](https://github.com/ISISNeutronMuon/digital-muon-pipeline/tree/main/nexus-writer) is in development as part of the SuperMuSR streaming pipeline.

The overall architecture is described by the [pipeline documentation](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/architecture/index.html). For background about the file-format the SuperMuSR filewriter was designed to write, see [file format ADR](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/adrs/0009-nexus-file-format.html).

This option for our filewriter implementation proposes using the SuperMuSR filewriter as a technical foundation, either forking it or working with the SuperMuSR team to incrementally adapt the SuperMuSR filewriter for neutron needs.

### Fit with requirements

#### NeXus file format

For this comparison, POLREF run POLREF00046184.nxs was written using both the ICP and the SuperMuSR filewriter, with minimal changes to support neutron data.

Generally: the SuperMuSR filewriter **hard-codes the structure of a Muon Nexus file**. The Muon Nexus format implemented by the current SuperMuSR filewriter differs substantially from existing ICP-written files, and from Neutron files. Adopting the SuperMuSR filewriter would therefore be an almost total re-write of everything under `nexus_structure`, while retaining much of the 'plumbing'.

A major initial decision would be whether to stick with the existing hard-coding of file structure, or to move to a dynamic `nexus_structure`, which is a **major** refactor but gives several requirements an immediate viable implementation path.

- [Neutron event data](https://github.com/isisComputingGroup/datastreaming/issues/83)
  * Event data in general is **partially supported**
    * The Kafka schema used is currently SuperMuSR-specific, but [an incomplete branch to support ev44](https://github.com/ISISNeutronMuon/digital-muon-pipeline/tree/ev44_support) exists
    * The key datasets are written, but with a number of differences and omissions relative to ICP files. As an example, the `total_counts` dataset is absent.
    * There is support for writing a per-frame `veto_flags` dataset in `NXevent_data`, but there does not appear to be a corresponding dataset for the vetoes a user had enabled at data acquisition time.
  * To support this, we would need to:
    * Implement a neutron-specific `ev44` event-data writer, reusing much of the code in the existing `aev2` support, or alternatively, refactor the existing event_data support to cater for both `ev44` and `aev2`
    * Add support for the small number of missing datasets (for example total_counts).
- [Neutron histogram data](https://github.com/isisComputingGroup/datastreaming/issues/84)
  * **Unsupported** in the filewriter itself
    * The SuperMuSR pipeline generates histograms as a separate step from event-mode Nexus file writing. This is instead done by [MNeuEventLib](https://github.com/ISISMuon/MNeuEventLib) for SuperMuSR.
  * To best support all combinations of files including pure-event, pure-histogram, event+histogram, detectors in event mode with monitors in histogram mode etc, we would want to add support for writing a histogrammed version of the event-mode data in the filewriter directly. This would require new code.
- [Static metadata](https://github.com/isisComputingGroup/datastreaming/issues/87)
  * This is **mostly unsupported**
    * The majority of the datasets listed do not exist
    * For some datasets which *are* written, the meaning is different from ICP files.
  * To support this, we would need to either:
    * Support a dynamic nexus_structure and include this information in the run start message
    * Individually add support for each of the listed datasets; the datasets are simple, static pieces of metadata, so writing each one is not a large task.
- [Dynamic metadata](https://github.com/isisComputingGroup/datastreaming/issues/88)
  * **Unsupported**
    * None of the listed datasets are written in a similar way to the existing ICP files.
    * Some streamed `f144`s are inserted into `runlog` (e.g. `/raw_data_1/runlog/IN:POLREF:DAE:RAWFRAMES`), but these rely on calculations performed in the ICP and so will not work for a pure-streaming system without an ICP. As the calculations are done in a different place, the `runlog` may therefore be inconsistent with the event data actually written to file.
  * To support this:
    * New code would need to be written for each piece of dynamic metadata. Most of these pieces of code are small and localised changes, but there are a reasonable number of them to implement.
- [SELog / blocks](https://github.com/isisComputingGroup/datastreaming/issues/89) - ensure to include the 'going back in time' requirement
  * This is **partially supported**
    * Values and timestamps are written
    * Blocks which do not update during a run are missed.
    * There is some support in code for alarm datasets.
    * Units are missing on all value datasets for all logs; no support for `un00` is currently present.
  * To support this, we would need to:
    * Ensure the filewriter can [get the list of blocks needing to be written from somewhere; whether a `nexus_structure` or some other mechanism](https://github.com/ISISNeutronMuon/digital-muon-pipeline/issues/423)
    * Make [a change](https://github.com/ISISNeutronMuon/digital-muon-pipeline/issues/424) to 'go back' to the most recent block update on run start, to ensure blocks which do not change get their most recent value and timestamp inserted to cover run start.
- [EPICS PutLog](https://github.com/isisComputingGroup/datastreaming/issues/91)
  * **Unsupported**.
  * To support this, we would need to write new code, which would likely take in these entries as a `vs00` schema and write them as an `NXlog` or `NXtextlog`. This would reuse much of the same logic as a sample environment block writer.
- [`runlog/icp_event`](https://github.com/isisComputingGroup/datastreaming/issues/92)
  * **Partially supported**. 
    * Some support for "internally generated events" exists, which has *some*, but not all, of the semantics of the ICP runlog. However, it is not in a format which mantid needs for Neutron data.
  * To support this, we would need to refactor the existing support to more closely align/emulate the previous ICP behaviour.
- [Period-specific metadata](https://github.com/isisComputingGroup/datastreaming/issues/93)
  * **Unsupported** (except for some very minimal metadata).
  * To support this, we would need to add new support for it; many datasets would be comparable to other 'dynamic metadata' datasets, a few would be static metadata (covered by either `nexus_structure` or explicit support).
- [`isis_vms_compat`](https://github.com/isisComputingGroup/datastreaming/issues/94)
  * **Unsupported**.
  * To support this, we would need to add support for 'faking' `isis_vms_compat` data from the event-stream and writing it to file.
  * Some of this data is static and known in advance; in this case, it could be included in the `nexus_structure` (if that mechanism is added). Otherwise custom logic will be needed for each dataset.
- [Monitor data](https://github.com/isisComputingGroup/datastreaming/issues/95)
  * **Unsupported** (not applicable to the Muon beamlines for which this filewriter was designed)
  * To support this, we would need to add specific monitor-writing logic. This would likely share significant components of event_data or histogramming logic, depending on the mode monitors are being written in (which is ultimately user-configurable).
- [`instrument` group](https://github.com/isisComputingGroup/datastreaming/issues/97)
  * **Mostly unsupported**.
    * Some datasets created (minimally) under `instrument/source`, but most are not populated with any meaningful data
    * All datasets under `instrument/dae` unsupported
  * To support this, we would need to either:
    * Add support for a dynamic `nexus_structure`, rather than the existing hard-coded structure, which would be a significant refactor
    * Individually add support for each of the listed datasets; the datasets are relatively simple/static pieces of metadata, so writing each one is not a large task.
- [Instrument-specific nexus extras e.g. `instrument_components.nxs`](https://github.com/ISISComputingGroup/DataStreaming/issues/106)
  * **Unsupported**. 
  * To support this, we would need to either:
    * Add support for a dynamic `nexus_structure`, rather than the existing hard-coded structure, which would be a significant refactor
    * A simpler but less flexible change to always copy in datasets from a source nexus file to the written file on run start - which meets this requirement, but does not help with other similar differences in expected nexus structure.

#### Downstream workflows

- [File archiving workflows](https://github.com/ISISComputingGroup/DataStreaming/issues/85)
  * **Partially supported**
    * The SuperMuSR filewriter [does include some support for archival workflows](https://github.com/ISISNeutronMuon/digital-muon-pipeline/blob/main/nexus-writer/src/flush_to_archive.rs) in the form of copying to a network drive.
    * There is no support for checksumming present.
  * To support this, we would need to either:
    * Add full support for archiving workflows inside the filewriter.
    * Alternatively, emit suitable `wrdn` messages and implement archiving logic in a downstream process. Emitting *minimal* `wrdn` messages should be a small amount of work.
- [Generating journals](https://github.com/isisComputingGroup/datastreaming/issues/86)
  * **Unsupported**
    * No support for generating/updating journals is present.
  * To support this, we would need to:
    * Ensure suitable `wrdn` messages are emitted, for a downstream process to update the instrument journal. To avoid downstream processes needing to open a potentially large nexus file only to extract minimal metadata, it would be best if the `wrdn` messages included the relevant pieces of dynamic metadata (e.g. total counts, micro-amp hours). No similar concept currently exists.

#### [Intermediate and autosave files](https://github.com/isisComputingGroup/datastreaming/issues/96)

- **Unsupported**
  * There does not appear to be any support for these concepts.
  * The file is flushed to disk incrementally.
  * SWMR support is not being used.
  * This filewriter uses a filewriter-per-instrument, so we cannot use an approach of generating a parallel write-request with different start/stop times.
- To support this, we would need to:
  * Introduce a change to pause file-writing activity, copy a file, 'end' one copy, and then continue writing to the other file. The file is already flushed and self-consistent between messages.
  

#### [Deployment topology](https://github.com/isisComputingGroup/datastreaming/issues/98)

- [A SuperMuSR ADR](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/adrs/0004-core-technologies-for-in-flight-data-processing-software.html#context) states that their "components should not be tied to a specific means of deployment, orchestration or execution or execution environment". In practice, a Linux OS is assumed, but a specific deployment technology is not.
- The SuperMuSR filewriter operates in a filewriter-per-instrument configuration.

### Code quality

- The major dependencies are listed [here](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/adrs/0004-core-technologies-for-in-flight-data-processing-software.html#decision): Rust, Nix, OCI container images.
- A few unit tests exist, but the amount of testing is minimal compared to the amount of code.
- Reasonable inline documentation is present.
- The codebase looks organised in a reasonable way, but is part of a monorepo which would make adopting it as a standalone component more difficult. An initial task would be to extract the filewriter component from the monorepo; the coupling to the monorepo is not excessively tight.
- In places, there is tight coupling to how the SuperMuSR data pipeline operates, which may need refactoring to also be suitable for how the neutron data pipeline operates. Cross-process assumptions are likely to be the most risky area during refactoring, as neither team have expertise in the other team's data pipeline model (even though they are *architecturally* similar at a very high level).
- Collaboration with the SuperMuSR team is desirable, but the extent of the changes required to the SuperMuSR filewriter mean that this collaboration is likely best achieved in either the greenfield or ESS options, aiming for a design that can cater for both the Neutron and SuperMuSR cases. The SuperMuSR team have indicated a willingness to collaborate on a new filewriter, separate from the existing SuperMuSR codebase.

### Supportability

Supportability and error handling look to have been carefully considered:
- The SuperMuSR filewriter includes support for logging and metrics throughout.
- A number of error conditions [have been considered](https://github.com/ISISNeutronMuon/digital-muon-pipeline/blob/cbed81bb66bfd7ec53a6be64f8e3bf1dcc9c1eab/nexus-writer/src/error.rs).
- Code-level error-handling strategies are [documented](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/adrs/0010-revised-error-handling-using-miette.html).
- Some known assumptions [are documented](https://github.com/ISISNeutronMuon/digital-muon-pipeline/tree/main/nexus-writer).
- [ADRs are available](https://isisneutronmuon.github.io/digital-muon-pipeline-docs/adrs/0001-record-architecture-decisions.html) for some major decisions, many of which relate to supportability infrastructure.
