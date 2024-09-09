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

    Enum.reduce(t1.rows, Table.new(), fn t1_row, result_table ->
      Enum.reduce(t2.rows, result_table, fn t2_row, result_table ->
        row_t1 =
          t1_row
          |> Map.keys()
          |> Enum.map(fn k ->
            cn = if t1.name, do: "#{t1.name}.#{k}", else: k
            {cn, t1_row[k]}
          end)
          |> Map.new()

        row_t2 =
          t2_row
          |> Map.keys()
          |> Enum.map(fn k ->
            cn = if t2.name, do: "#{t2.name}.#{k}", else: k
            {cn, t2_row[k]}
          end)
          |> Map.new()

        row =
          row_t1
          |> Map.merge(row_t2)
          |> Map.put(:_table_rows, [t1_row, t2_row])

        Table.append(result_table, row)
      end)
    end)
  end

  def inner_join(db, t1, t2, pred) do
    rows =
      db
      |> cross_join(t1, t2)
      |> Map.get(:rows)
      |> Enum.filter(pred)

    Table.new(nil, rows)
  end

  def left_join(db, t1, t2, pred) do
    t1 = from(db, t1)
    t2 = from(db, t2)
    cp = cross_join(t1, t2)

    Enum.reduce(t1.rows, Table.new(nil), fn t1_row, result ->
      cp_t1 = Enum.filter(cp.rows, fn %{_table_rows: tr} -> Enum.any?(tr, &(&1 == t1_row)) end)

      case Enum.filter(cp_t1, pred) do
        [_ | _] = match ->
          Table.append(result, match)

        [] ->
          t1_v =
            t1_row
            |> Map.keys()
            |> Enum.map(&{"#{t1.name}.#{&1}", t1_row[&1]})
            |> Map.new()

          t2_v =
            t2.rows
            |> List.first(%{})
            |> Map.keys()
            |> Enum.map(&{"#{t2.name}.#{&1}", nil})
            |> Map.new()

          Table.append(result, Map.merge(t1_v, t2_v))
      end
    end)
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
end
