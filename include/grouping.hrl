-record(grouping, {
   type,      %% field | shuffle | global
   source,    %% string - name of spout or bolt
   args = []  %% grouping specific arguments
}).