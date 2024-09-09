defmodule SqltoyElixir.Database do
  @moduledoc """
  A database structure for SQLToy.
  """

  alias SqltoyElixir.Table

  import SqltoyElixir.Util

  defstruct tables: %{}

  def init, do: with_db(nil, fn _ -> create() end)

  def create, do: %__MODULE__{}

  def create_table(database \\ nil, name), do: with_db(database, &_create_table(&1, name))

  def drop_table(database \\ nil, name), do: with_db(database, &_drop_table(&1, name))

  def insert_into(database \\ nil, name, rows),
    do: with_db(database, &_insert_into(&1, name, rows))

  def update(database \\ nil, name, set, pred \\ nil),
    do: with_db(database, &_update(&1, name, set, pred))

  def from(database \\ nil, names)

  def from(db, [table]), do: from(db, table)
  def from(db, [head | tail]), do: cross_join(db, from(db, head), from(db, tail))
  def from(_, %Table{} = table), do: table
  def from(nil, name), do: from(Process.get("$database"), normalize(name))
  def from(db, name), do: Map.fetch!(db.tables, normalize(name))

  def cross_join(database \\ nil, t1, t2) do
    t1 = from(database, t1)
    t2 = from(database, t2)

    for t1_row <- t1.rows, t2_row <- t2.rows, into: %Table{} do
      left = namespaced_row(t1, t1_row)
      right = namespaced_row(t2, t2_row)

      left
      |> Map.merge(right)
      |> Map.put(:_table_rows, [t1_row, t2_row])
    end
  end

  def inner_join(db, t1, t2, pred) do
    %Table{rows: Enum.filter(cross_join(db, t1, t2).rows, pred)}
  end

  def left_join(db, t1, t2, pred) do
    t1 = from(db, t1)
    t2 = from(db, t2)
    cp = cross_join(t1, t2)

    for t1_row <- t1.rows, into: %Table{} do
      cp_t1 = Enum.filter(cp.rows, fn %{_table_rows: tr} -> Enum.any?(tr, &(&1 == t1_row)) end)

      case Enum.filter(cp_t1, pred) do
        [_ | _] = match ->
          match

        [] ->
          left = namespaced_row(t1, t1_row)
          right = namespaced_row(t2, List.first(t2.rows, %{}), true)

          Map.merge(left, right)
      end
    end
  end

  def right_join(db, t1, t2, pred), do: left_join(db, t2, t1, pred)

  defp _create_table(database, name) do
    name = normalize(name)

    if Map.has_key?(database.tables, name) do
      raise "Table #{name} already exists in database."
    end

    update_database_table(database, Table.new(name))
  end

  defp _drop_table(database, name) do
    name = normalize(name)

    if Map.has_key?(database.tables, name) do
      %{database | tables: Map.drop(database.tables, name)}
    else
      raise("Table #{name} does not exist in database.")
    end
  end

  defp _insert_into(database, name, row) do
    update_database_table(database, Table.append(database.tables[name], row))
  end

  defp _update(database, name, set, pred) do
    update_database_table(database, Table.update(database.tables[name], set, pred))
  end

  defp update_database_table(database, table) do
    %{database | tables: Map.put(database.tables, table.name, table)}
  end

  defp with_db(nil, fun) do
    db = fun.(Process.get("$database"))

    Process.put("$database", db)

    db
  end

  defp with_db(db, fun), do: fun.(db)

  defp namespaced_row(table, row, nilify? \\ false)

  defp namespaced_row(table, row, true) do
    for {k, _v} <- row, into: %{}, do: {namespaced_key(table, k), nil}
  end

  defp namespaced_row(table, row, _) do
    for {k, v} <- row, into: %{}, do: {namespaced_key(table, k), v}
  end

  defp namespaced_key(%Table{name: nil}, key), do: key
  defp namespaced_key(%Table{name: name}, key), do: "#{name}.#{key}"
end
