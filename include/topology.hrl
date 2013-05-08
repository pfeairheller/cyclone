-record(topology, {
  name,              %% string
  spout_specs = [],  %% [spout_spec records]
  bolt_specs = []   %% [bolt_spec records]
}).

-record(bolt_spec, {
  id,                %% string
  bolt,              %% tuple {module that implements bolt behavior, args}
  workers,           %% int - number of workers - maybe called in redundency?
  stateful = false,  %% true | false
  groupings = []     %% [#groupings]
}).

-record(grouping, {
   type,             %% field | shuffle | global
   source,           %% string - name of spout or bolt
   args = []         %% grouping specific arguments
}).

-record(spout_spec, {
  id,                %% string
  spout,             %% tuple { module that implements spout behavior, args}
  reliable = false,  %% true | false - whether the spout can replay a failed tuple.
  workers            %% int - number of workers
}).
