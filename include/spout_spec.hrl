-record(spout_spec, {
  id,               %% string
  spout,            %% tuple { module that implements spout behavior, args}
  reliable = false, %% true | false - whether the spout can replay a failed tuple.
  workers           %% int - number of workers
}).