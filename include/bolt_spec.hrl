-record(bolt_spec, {
  id,             %% string
  bolt,           %% tuple {module that implements bolt behavior, args}
  workers,        %% int - number of workers - maybe called in redundency?
  groupings = []  %% [#groupings]
}).