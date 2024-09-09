defmodule SqltoyElixir.Util do
  def normalize(row) when is_map(row), do: Map.new(row, fn {k, v} -> {normalize(k), v} end)

  def normalize(name) when is_atom(name), do: Atom.to_string(name)
  def normalize(name) when is_binary(name), do: name
  def normalize([_ | _] = name), do: List.to_string(name)
end
