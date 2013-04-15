-record(spout_spec, {
  id,         %% string
  spout,      %% tuple { module that implements spout behavior, args}
  workers     %% int - number of workers
}).