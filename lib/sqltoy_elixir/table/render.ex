defmodule SqltoyElixir.Table.Render do
  @moduledoc "Render a Table as a formatted string"

  alias SqltoyElixir.Table

  def csv(%Table{} = table) do
    columns = columns_for(table)
    header = Enum.join(columns, ",")

    body =
      Enum.map(table.rows, fn row ->
        columns
        |> Enum.map(fn column ->
          case row[column] do
            list when is_list(list) -> ~s("[#{Enum.join(",")}]")
            value -> to_string(value)
          end
        end)
        |> Enum.join(",")
      end)

    Enum.join([header | body], "\r\n") <> "\r\n"
  end

  @chars %{
    top: "─",
    top_mid: "┬",
    top_left: "┌",
    top_right: "┐",
    bottom: "─",
    bottom_mid: "┴",
    bottom_left: "└",
    bottom_right: "┘",
    left: "│",
    left_mid: "├",
    mid: "─",
    mid_mid: "┼",
    right: "│",
    right_mid: "┤",
    middle: "│"
  }

  def table(%Table{} = table) do
    columns = columns_for(table)

    column_widths =
      Map.new(columns, fn column ->
        length =
          table.rows
          |> Enum.map(&to_string(&1[column]))
          |> Enum.max_by(&String.length/1)
          |> String.length()

        col_length = String.length(to_string(column))

        length = if col_length > length, do: col_length, else: length

        {column, length}
      end)

    column_count = Enum.count(columns)
    col_sep_count = column_count + 1
    col_spc_count = column_count * 2
    full_width = Enum.sum(Map.values(column_widths)) + col_sep_count + col_spc_count

    context = %{columns: columns, widths: column_widths, full_width: full_width}

    [
      table_top(table, context),
      table_body(table.rows, context),
      table_bottom(context)
    ]
  end

  defp table_top(table, context) do
    [
      table_name(table, context),
      table_top_line(context, is_nil(table.name))
    ]
  end

  defp table_name(%{name: nil}, _context), do: []

  defp table_name(%{name: name}, context) do
    name_len = String.length(name)
    space_left = context.full_width - 4 - name_len
    half_space = div(space_left, 2)
    rem_space = rem(space_left, 2)

    [
      # Table name top line
      [
        @chars.top_left,
        String.duplicate(@chars.top, context.full_width - 2),
        @chars.top_right,
        "\n"
      ],
      # Table name centered
      [
        @chars.left,
        String.duplicate(" ", half_space + rem_space + 1),
        name,
        String.duplicate(" ", half_space + 1),
        @chars.right,
        "\n"
      ]
    ]
  end

  defp table_top_line(context, missing_table_name?) do
    [left, right] =
      if missing_table_name? do
        [@chars.top_left, @chars.top_right]
      else
        [@chars.left_mid, @chars.right_mid]
      end

    [
      # line above the header
      [
        left,
        context.columns
        |> Enum.map(&String.duplicate(@chars.mid, context.widths[&1] + 2))
        |> Enum.join(@chars.top_mid),
        right,
        "\n"
      ],
      # header; this should be centered, but whatever
      [
        @chars.left,
        table_row(Map.new(context.columns, &{&1, to_string(&1)}), context),
        @chars.right,
        "\n"
      ],
      # separator between header and body
      [
        left,
        context.columns
        |> Enum.map(&String.duplicate(@chars.mid, context.widths[&1] + 2))
        |> Enum.join(@chars.mid_mid),
        right,
        "\n"
      ]
    ]
  end

  defp table_row(row, context) do
    context.columns
    |> Enum.map(fn column ->
      case row[column] do
        value when is_number(value) ->
          value = to_string(value)

          [
            String.duplicate(" ", context.widths[column] - String.length(value) + 1),
            value,
            " "
          ]

        value ->
          value = to_string(value)

          [
            " ",
            value,
            String.duplicate(" ", context.widths[column] - String.length(value) + 1)
          ]
      end
    end)
    |> Enum.join(@chars.middle)
  end

  defp table_body(rows, context) do
    Enum.map(rows, fn row ->
      [
        @chars.left,
        table_row(row, context),
        @chars.right,
        "\n"
      ]
    end)
  end

  defp table_bottom(context) do
    [
      @chars.bottom_left,
      context.columns
      |> Enum.map(&String.duplicate(@chars.mid, context.widths[&1] + 2))
      |> Enum.join(@chars.bottom_mid),
      @chars.bottom_right,
      "\n"
    ]
  end

  defp columns_for(%{_select: nil, rows: [first | _]}) do
    first
    |> Map.keys()
    |> Enum.filter(&is_binary/1)
  end

  defp columns_for(%{_select: selected}), do: selected
end
