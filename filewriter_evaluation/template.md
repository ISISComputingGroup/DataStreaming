## Option

### Background

Some context about where this filewriter comes from, where it is used, what this implementation option looks like at a very high level.

### Fit with requirements

For each of the sections below, fill out a brief high-level view of how our requirement would be met using this filewriter, and a summary of the extent of work that would need to be done to add the relevant functionality.

#### NeXus file format

Summarise which of the following areas of our NeXus file format are already implemented, partially implemented, or would need implementing.
Document whether any specific datasets would be particularly difficult to add, and areas that would need special architectural attention.

- [Neutron event data](https://github.com/isisComputingGroup/datastreaming/issues/83)
- [Neutron histogram data](https://github.com/isisComputingGroup/datastreaming/issues/84)
- [Static metadata](https://github.com/isisComputingGroup/datastreaming/issues/87)
- [Dynamic metadata](https://github.com/isisComputingGroup/datastreaming/issues/88)
- [SELog / blocks](https://github.com/isisComputingGroup/datastreaming/issues/89) - ensure to include the 'going back in time' requirement
- [EPICS PutLog](https://github.com/isisComputingGroup/datastreaming/issues/91)
- [`runlog/icp_event`](https://github.com/isisComputingGroup/datastreaming/issues/92)
- [Period-specific metadata](https://github.com/isisComputingGroup/datastreaming/issues/93)
- [`isis_vms_compat`](https://github.com/isisComputingGroup/datastreaming/issues/94) - for the purposes of evaluation, imagine that we will need to write a limited subset of the current `isis_vms_compat` datasets.
- [Monitor data](https://github.com/isisComputingGroup/datastreaming/issues/95)
- [`instrument` group](https://github.com/isisComputingGroup/datastreaming/issues/97)
- [Instrument-specific nexus extras e.g. `instrument_components.nxs`](https://github.com/ISISComputingGroup/DataStreaming/issues/106)

#### Downstream workflows

Summarise how this filewriter would support downstream workflows, which are part of the file-writing infrastructure as a whole but may not necessarily be part of the filewriter directly.

For example:
- [File archiving workflows](https://github.com/ISISComputingGroup/DataStreaming/issues/85)
- [Generating journals](https://github.com/isisComputingGroup/datastreaming/issues/86)

#### [Intermediate and autosave files](https://github.com/isisComputingGroup/datastreaming/issues/96)

Summarise how we would meet requirements for autosave and intermediate files using this filewriter.

#### [Deployment topology](https://github.com/isisComputingGroup/datastreaming/issues/98)

Summarise how this filewriter expects to be deployed. Include scalability considerations.
For example: is it a dynamic-pool architecture, a static-pool architecture, or a filewriter-per-instrument architecture? Does it strongly assume a specific deployment technology?

### Code quality

Add some general notes about the quality of the codebase. 
Including areas like: high-level & code-level documentation, unit & system testing, codebase layout, major dependencies.
For external repositories, include information on how feasible a genuine collaboration is, as opposed to a local forked repository.

### Supportability

Add some general notes about how supportable the filewriter would be in production, focusing on error scenarios.
Including areas like: logging, metrics, error handling, error recovery, hardening against invalid/unexpected data.
