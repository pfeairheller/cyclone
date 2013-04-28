-record(bolt_spec, {
  id,         %% string
  bolt,       %% tuple {module that implements bolt behavior, args}
  stateful,   %% true | false
  workers,    %% int - number of workers
  groupings   %% [#groupings]
}).