defmodule SqltoyElixir do
  @moduledoc """
  Documentation for `SqltoyElixir`.

  This is my implementation of [SQLToy][1] in Elixir.

  [1]: https://github.com/weinberg/SQLToy/wiki
  """

  alias SqltoyElixir.Database
  alias SqltoyElixir.Table

  defdelegate init, to: Database
  defdelegate create_table(database \\ nil, name), to: Database
  defdelegate drop_table(database \\ nil, name), to: Database
  defdelegate insert_into(database \\ nil, name, rows), to: Database
  defdelegate from(database \\ nil, name), to: Database
  defdelegate cross_join(database \\ nil, t1, t2), to: Database
  defdelegate inner_join(database \\ nil, t1, t2, pred), to: Database
  defdelegate join(database \\ nil, t1, t2, pred), to: Database, as: :inner_join
  defdelegate left_join(database \\ nil, t1, t2, pred), to: Database
  defdelegate right_join(database \\ nil, t1, t2, pred), to: Database
  defdelegate update(database \\ nil, table, set, pred), to: Database

  def where(%Table{} = table, pred), do: Table.where(table, pred)

  def where(database, table, pred) when is_nil(database) or is_struct(database, Database),
    do: where(from(database, table), pred)

  def select(%Table{} = table, columns), do: select(table, columns, %{})
  def select(%Table{} = table, columns, aliases), do: Table.select(table, columns, aliases)
  def select(database, table, columns), do: select(database, table, columns, %{})

  def select(database, table, columns, aliases)
      when is_nil(database) or is_struct(database, Database),
      do: select(from(database, table), columns, aliases)

  defdelegate distinct(t, c), to: Table
  defdelegate group_by(t, c), to: Table
  defdelegate order_by(t, r), to: Table
  defdelegate offset(t, offset), to: Table
  defdelegate limit(t, limit), to: Table
  defdelegate having(t, pred), to: Table
  defdelegate array_agg(t, c), to: Table
  defdelegate avg(t, c), to: Table
  defdelegate max(t, c), to: Table
  defdelegate min(t, c), to: Table
  defdelegate count(t, c), to: Table

  defdelegate csv(t), to: Table
  defdelegate table(t), to: Table
end
