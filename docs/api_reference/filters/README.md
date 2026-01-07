# Neptune Query API filters

When calling a fetching method, use filter classes to specify the project contents that you're interested in:

- To define detailed criteria for experiments or runs to search, use `Filter`.
- For methods that return dataframes, use `AttributeFilter` to specify attributes to include as columns.
- To specify exact criteria for an attribute and its type, use `Attribute`.
