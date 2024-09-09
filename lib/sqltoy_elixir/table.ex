defmodule SqltoyElixir.Table do
  @moduledoc """
  A table structure for SQLToy.
  """

  @us "\u0031"

  alias SqltoyElixir.Table.Render

  defstruct [:name, rows: [], _select: nil]

  def new(name \\ nil, rows \\ []), do: %__MODULE__{name: name, rows: rows}

  def csv(table), do: Render.csv(table)

  def table(table), do: Render.table(table)

  def append(%__MODULE__{} = table, rows), do: %{table | rows: table.rows ++ normalize_rows(rows)}

  def update(%__MODULE__{} = table, set, pred \\ nil) do
    new_rows =
      Enum.map(table.rows, fn row ->
        update? = if pred, do: pred.(row), else: true

        if update? do
          Enum.reduce(Map.keys(set), row, &Map.put(&2, &1, set[&1]))
        else
          row
        end
      end)

    %{table | rows: new_rows}
  end

  def where(%__MODULE__{} = table, pred), do: %{table | rows: Enum.filter(table.rows, pred)}

  def select(%__MODULE__{} = table, columns, aliases \\ %{}) do
    columns = Enum.map(columns, &to_string/1)
    selected = Enum.map(columns, &Map.get(aliases, &1, &1))

    colnames =
      columns
      |> Enum.map(&{&1, Map.get(aliases, &1, &1)})
      |> Map.new()

    new_rows =
      Enum.map(table.rows, fn row ->
        columns
        |> Enum.map(&{colnames[&1], row[&1]})
        |> Map.new()
      end)

    %__MODULE__{name: table.name, rows: new_rows, _select: selected}
  end

  def group_by(%__MODULE__{} = table, group_bys) do
    group_bys = Enum.map(group_bys, &to_string/1)

    key_rows =
      Enum.reduce(table.rows, %{}, fn row, key_rows ->
        key =
          group_bys
          |> Enum.map(&row[&1])
          |> Enum.join(@us)

        if Map.has_key?(key_rows, key) do
          Map.put(key_rows, key, key_rows[key] ++ [row])
        else
          Map.put(key_rows, key, [row])
        end
      end)

    result_rows =
      Enum.reduce(Map.keys(key_rows), [], fn key, result_rows ->
        result_row =
          Enum.reduce(group_bys, %{_grouped_values: key_rows[key]}, fn group_by, result_row ->
            Map.put(result_row, group_by, Enum.at(key_rows[key], 0)[group_by])
          end)

        [result_row | result_rows]
      end)

    %{table | rows: Enum.reverse(result_rows)}
  end

  def having(%__MODULE__{} = table, pred), do: %{table | rows: Enum.filter(table.rows, pred)}

  def distinct(%__MODULE__{} = table, columns) do
    columns = Enum.map(columns, &to_string/1)

    distinct =
      table.rows
      |> Enum.map(fn row ->
        key =
          columns
          |> Enum.map(&row[&1])
          |> Enum.join(@us)

        {key, row}
      end)
      |> Map.new()

    new_rows =
      distinct
      |> Map.keys()
      |> Enum.map(fn key ->
        columns
        |> Enum.map(&{&1, distinct[key][&1]})
        |> Map.new()
      end)

    %{table | rows: new_rows}
  end

  def order_by(%__MODULE__{} = table, rel), do: %{table | rows: Enum.sort(table.rows, rel)}

  def offset(%__MODULE__{} = table, offset),
    do: %{table | rows: Enum.slice(table.rows, Range.new(offset, -1, 1))}

  def limit(%__MODULE__{} = table, limit), do: %{table | rows: Enum.slice(table.rows, 0, limit)}

  def array_agg(%__MODULE__{} = table, column) do
    aggregate(table, column, "ARRAY_AGG", fn pick ->
      v =
        pick
        |> Enum.map(&to_string/1)
        |> Enum.join(",")

      "[#{v}]"
    end)
  end

  def avg(%__MODULE__{} = table, column) do
    aggregate(table, column, "AVG", &(Enum.sum(&1) / Enum.count(&1)))
  end

  def max(%__MODULE__{} = table, column) do
    aggregate(table, column, "MAX", &Enum.max(&1))
  end

  def min(%__MODULE__{} = table, column) do
    aggregate(table, column, "MIN", &Enum.min(&1))
  end

  def count(%__MODULE__{} = table, column) do
    aggregate(table, column, "COUNT", &Enum.count(&1))
  end

  defp aggregate(table, column, name, fun) do
    column = to_string(column)

    new_rows =
      Enum.map(table.rows, fn row ->
        pick = Enum.map(row._grouped_values, & &1[column])
        Map.put(row, "#{name}(#{column})", fun.(pick))
      end)

    %{table | rows: new_rows}
  end

  defp normalize_rows(rows) when is_list(rows), do: Enum.map(rows, &normalize_row(&1))
  defp normalize_rows(row) when is_map(row), do: normalize_rows([row])

  defp normalize_row(row) when is_map(row) do
    Map.new(row, fn
      {k, v} when k in [:_grouped_values, :_table_rows] -> {k, v}
      {k, v} -> {to_string(k), v}
    end)
  end

  defimpl Collectable do
    alias SqltoyElixir.Table

    def into(table) do
      {table, &collector/2}
    end

    def collector(table, {:cont, elem}), do: Table.append(table, elem)
    def collector(table, :done), do: table
    def collector(_table, :halt), do: :ok
  end
end
