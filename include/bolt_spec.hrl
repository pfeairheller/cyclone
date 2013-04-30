-record(bolt_spec, {
  id,             %% string
  bolt,           %% tuple {module that implements bolt behavior, args}
  groupings = []  %% [#groupings]
}).