-record(grouping, {
   type,     %% field | shuffle | global
   source,   %% string - name of spout or bolt
   dest      %% tuple { name of bolt, args to grouping }
}).