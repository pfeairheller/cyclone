-record(topology, {
  name,         %% string
  spout_specs,  %% [spout_spec records]
  bolts_specs,  %% [bolt_spec records]
  groupings     %% [grouping records]
}).
