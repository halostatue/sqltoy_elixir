defmodule SqltoyElixirTest do
  use ExUnit.Case

  doctest SqltoyElixir

  alias SqltoyElixir
  alias SqltoyElixir.Database
  alias SqltoyElixir.Table

  import SqltoyElixir

  test "init/0 sets $database to Database.create()" do
    assert nil == Process.get("$database")
    assert %Database{} == init()
    assert %Database{} == Process.get("$database")
  end

  setup do
    {:ok, db: Database.create()}
  end

  test "create_table/2", %{db: db} do
    assert %Database{tables: %{"stories" => %Table{name: "stories", rows: []}}} ==
             create_table(db, "stories")
  end

  describe "insert_into/3" do
    setup ctx do
      {:ok, db: create_table(ctx.db, "stories")}
    end

    test "with list of records", %{db: db} do
      assert %Database{
               tables: %{
                 "stories" => %Table{
                   name: "stories",
                   rows: [
                     %{
                       "id" => 1,
                       "name" => "The Elliptical Machine that ate Manhattan",
                       "author_id" => 1
                     },
                     %{"id" => 2, "name" => "Queen of the Bats", "author_id" => 2},
                     %{"id" => 3, "name" => "ChocoMan", "author_id" => 3}
                   ]
                 }
               }
             } ==
               insert_into(db, "stories", [
                 %{id: 1, name: "The Elliptical Machine that ate Manhattan", author_id: 1},
                 %{id: 2, name: "Queen of the Bats", author_id: 2},
                 %{id: 3, name: "ChocoMan", author_id: 3}
               ])
    end

    test "with one record", %{db: db} do
      assert %Database{
               tables: %{
                 "stories" => %Table{
                   name: "stories",
                   rows: [%{"id" => 4, "name" => "Something", "author_id" => 5}]
                 }
               }
             } ==
               insert_into(db, "stories", %{id: 4, name: "Something", author_id: 5})
    end
  end

  describe "from/2" do
    setup :cross_data

    test "returns the selectable table data", ctx do
      assert %Table{name: "test1", rows: [%{"c" => "A"}, %{"c" => "B"}]} == from(ctx.db, "test1")
    end

    test "from with multiple tables is a cross join", ctx do
      assert from(ctx.db, ["test1", "test2"]) == cross_join(ctx.db, "test1", "test2")
    end

    test "from can make n-way cross joins", ctx do
      assert "test1.c,test2.c,test3.c\r\nA,1,X\r\nA,1,Y\r\nA,2,X\r\nA,2,Y\r\nB,1,X\r\nB,1,Y\r\nB,2,X\r\nB,2,Y\r\n" ==
               from(ctx.db, ["test1", "test2", "test3"])
               |> csv()
    end
  end

  describe "select/2" do
    setup :games_data
    setup :employee_data

    test "select type, result from games", ctx do
      assert "type,result\r\nChess,Win\r\nChess,Loss\r\nCheckers,Loss\r\nDominos,Loss\r\nBattleship,Win\r\n" ==
               from(ctx.db, "games")
               |> select(["type", "result"])
               |> csv()
    end

    test "select with alias on cross join", %{db: db} do
      games = from(db, "games")
      player = from(db, "player")

      result =
        games
        |> join(player, &(&1["games.player_id"] == &1["player.id"]))
        |> select(["games.type", "player.name", "games.result"], %{
          "games.type" => "Game Type",
          "player.name" => "Player Name",
          "games.result" => "Win or Loss"
        })
        |> csv()

      assert Enum.join(
               [
                 "Game Type,Player Name,Win or Loss",
                 "Chess,Josh,Win",
                 "Chess,Josh,Loss",
                 "Checkers,Ruth,Loss",
                 "Dominos,Ruth,Loss",
                 "Battleship,Josh,Win",
                 ""
               ],
               "\r\n"
             ) ==
               result
    end

    test "select with aggregate functions", %{db: db} do
      result =
        db
        |> from("employee")
        |> group_by(["department_id", "status"])
        |> array_agg("name")
        |> SqltoyElixir.max("salary")
        |> count("status")
        |> select(["department_id", "status", "ARRAY_AGG(name)", "MAX(salary)", "COUNT(status)"])
        |> csv

      assert Enum.join(
               [
                 "department_id,status,ARRAY_AGG(name),MAX(salary),COUNT(status)",
                 "1,active,[Josh],50000,1",
                 "2,active,[Garth],35000,1",
                 "2,inactive,[Ruth],60000,1",
                 "3,inactive,[Michael],80000,1",
                 "4,active,[Greg],70000,1",
                 ""
               ],
               "\r\n"
             ) ==
               result

      # ┌───────────────┬────────────┬────────────────────────┬─────────────┬───────────────┐
      # │ department_id │   status   │    array_agg(name)     │ max(salary) │ count(status) │
      # ├───────────────┼────────────┼────────────────────────┼─────────────┼───────────────┤
      # │       1       │  inactive  │   [  Josh ,  Ruth  ]   │   200000    │       2       │
      # │       2       │   active   │       [  Jane  ]       │   160000    │       1       │
      # │       1       │   active   │      [  Elliot  ]      │   180000    │       1       │
      # │               │   active   │ [  Michael ,  Garth  ] │   200000    │       2       │
      # └───────────────┴────────────┴────────────────────────┴─────────────┴───────────────┘
    end
  end

  describe "group_by/2 and aggregate functions" do
    setup :employee_data

    test "group_by department_id, array_agg(name)", ctx do
      assert %Table{
               name: "employee",
               rows: [
                 %{
                   "ARRAY_AGG(name)" => "[Josh]",
                   "department_id" => 1,
                   _grouped_values: [_]
                 },
                 %{
                   "ARRAY_AGG(name)" => "[Ruth,Garth]",
                   "department_id" => 2,
                   _grouped_values: [_, _]
                 },
                 %{
                   "ARRAY_AGG(name)" => "[Michael]",
                   "department_id" => 3,
                   _grouped_values: [_]
                 },
                 %{
                   "ARRAY_AGG(name)" => "[Greg]",
                   "department_id" => 4,
                   _grouped_values: [_]
                 }
               ]
             } =
               ctx.db
               |> from("employee")
               |> group_by([:department_id])
               |> array_agg(:name)
    end

    test "group_by department_id, count(*)", ctx do
      assert %Table{
               name: "employee",
               rows: [
                 %{
                   "COUNT(*)" => 1,
                   "department_id" => 1,
                   _grouped_values: [_]
                 },
                 %{
                   "COUNT(*)" => 2,
                   "department_id" => 2,
                   _grouped_values: [_, _]
                 },
                 %{
                   "COUNT(*)" => 1,
                   "department_id" => 3,
                   _grouped_values: [_]
                 },
                 %{
                   "COUNT(*)" => 1,
                   "department_id" => 4,
                   _grouped_values: [_]
                 }
               ]
             } =
               ctx.db
               |> from("employee")
               |> group_by([:department_id])
               |> count("*")
    end

    test "group_by department_id, avg(salary)", ctx do
      assert %Table{
               name: "employee",
               rows: [
                 %{
                   "AVG(salary)" => 50000.0,
                   "department_id" => 1,
                   _grouped_values: [_]
                 },
                 %{
                   "AVG(salary)" => 47500.0,
                   "department_id" => 2,
                   _grouped_values: [_, _]
                 },
                 %{
                   "AVG(salary)" => 80000.0,
                   "department_id" => 3,
                   _grouped_values: [_]
                 },
                 %{
                   "AVG(salary)" => 70000.0,
                   "department_id" => 4,
                   _grouped_values: [_]
                 }
               ]
             } =
               ctx.db
               |> from("employee")
               |> group_by([:department_id])
               |> avg(:salary)
    end

    test "group_by department_id, max(salary)", ctx do
      assert %Table{
               name: "employee",
               rows: [
                 %{
                   "MAX(salary)" => 50000,
                   "department_id" => 1,
                   _grouped_values: [_]
                 },
                 %{
                   "MAX(salary)" => 60000,
                   "department_id" => 2,
                   _grouped_values: [_, _]
                 },
                 %{
                   "MAX(salary)" => 80000,
                   "department_id" => 3,
                   _grouped_values: [_]
                 },
                 %{
                   "MAX(salary)" => 70000,
                   "department_id" => 4,
                   _grouped_values: [_]
                 }
               ]
             } =
               ctx.db
               |> from("employee")
               |> group_by(["department_id"])
               |> SqltoyElixir.max("salary")
    end

    test "group_by department_id, min(salary)", ctx do
      assert %Table{
               name: "employee",
               rows: [
                 %{
                   "MIN(salary)" => 50000,
                   "department_id" => 1,
                   _grouped_values: [_]
                 },
                 %{
                   "MIN(salary)" => 35000,
                   "department_id" => 2,
                   _grouped_values: [_, _]
                 },
                 %{
                   "MIN(salary)" => 80000,
                   "department_id" => 3,
                   _grouped_values: [_]
                 },
                 %{
                   "MIN(salary)" => 70000,
                   "department_id" => 4,
                   _grouped_values: [_]
                 }
               ]
             } =
               ctx.db
               |> from("employee")
               |> group_by(["department_id"])
               |> SqltoyElixir.min("salary")
    end

    test "group_by department_id, count(*), having count(*) > 1", ctx do
      assert %Table{name: "employee", rows: [%{"COUNT(*)" => 2, "department_id" => 2}]} =
               ctx.db
               |> from("employee")
               |> group_by(["department_id"])
               |> count("*")
               |> having(&(&1["COUNT(*)"] > 1))
    end
  end

  describe "offset/2 and limit/2" do
    setup :employee_data

    test "limit(2)", ctx do
      assert %Table{name: "employee", rows: [%{"id" => 1}, %{"id" => 2}]} =
               ctx.db
               |> from("employee")
               |> limit(2)
    end

    test "offset(4)", ctx do
      assert %Table{name: "employee", rows: [%{"id" => 5}]} =
               ctx.db
               |> from("employee")
               |> offset(4)
    end

    test "offset+limit pagination", ctx do
      assert %Table{name: "employee", rows: [%{"id" => 1}, %{"id" => 2}]} =
               ctx.db
               |> from("employee")
               |> offset(0)
               |> limit(2)

      assert %Table{name: "employee", rows: [%{"id" => 3}, %{"id" => 4}]} =
               ctx.db
               |> from("employee")
               |> offset(2)
               |> limit(2)

      assert %Table{name: "employee", rows: [%{"id" => 5}]} =
               ctx.db
               |> from("employee")
               |> offset(4)
               |> limit(2)
    end
  end

  describe "update/3" do
    setup :employee_data

    test "update employee name", ctx do
      assert %Database{
               tables: %{"employee" => %Table{name: "employee", rows: [%{name: "JOSH"} | _]}}
             } =
               update(ctx.db, "employee", %{name: "JOSH"}, &(&1["name"] == "Josh"))
    end
  end

  describe "cross_join/3" do
    setup :cross_data

    test "manual cross join", ctx do
      assert "test1.c,test2.c\r\nA,1\r\nA,2\r\nB,1\r\nB,2\r\n" ==
               cross_join(ctx.db, "test1", "test2")
               |> csv()
    end
  end

  describe "inner_join/4" do
    setup :employee_data

    test "inner join employee, department, on department_id", %{db: db} do
      # SELECT * FROM employee JOIN department ON employee.department_id = department.id;

      assert Enum.join(
               [
                 "department.id,department.name,employee.department_id,employee.id,employee.name,employee.salary,employee.status",
                 "1,Sales,1,1,Josh,50000,active",
                 "2,Engineering,2,2,Ruth,60000,inactive",
                 "4,Consultants,4,3,Greg,70000,active",
                 "3,Management,3,4,Michael,80000,inactive",
                 "2,Engineering,2,5,Garth,35000,active",
                 ""
               ],
               "\r\n"
             ) ==
               inner_join(
                 db,
                 "employee",
                 "department",
                 &(&1["employee.department_id"] == &1["department.id"])
               )
               |> csv()
    end
  end

  describe "left_join/4" do
    setup :employee_data

    test "left join employee, department on department id", %{db: db} do
      # SELECT * FROM employee LEFT JOIN department ON employee.department_id = department.id
      assert Enum.join(
               [
                 "department.id,department.name,employee.department_id,employee.id,employee.name,employee.salary,employee.status",
                 "1,Sales,1,1,Josh,50000,active",
                 "2,Engineering,2,2,Ruth,60000,inactive",
                 "4,Consultants,4,3,Greg,70000,active",
                 "3,Management,3,4,Michael,80000,inactive",
                 "2,Engineering,2,5,Garth,35000,active",
                 ""
               ],
               "\r\n"
             ) ==
               left_join(
                 db,
                 "employee",
                 "department",
                 &(&1["employee.department_id"] == &1["department.id"])
               )
               |> csv()
    end
  end

  describe "right_join/4" do
    setup :employee_data

    test "right outer join on department id", %{db: db} do
      # SELECT * FROM employee RIGHT JOIN department ON employee.department_id = department.id;
      assert Enum.join(
               [
                 "department.id,department.name,employee.department_id,employee.id,employee.name,employee.salary,employee.status",
                 "1,Sales,1,1,Josh,50000,active",
                 "2,Engineering,2,2,Ruth,60000,inactive",
                 "2,Engineering,2,5,Garth,35000,active",
                 "3,Management,3,4,Michael,80000,inactive",
                 "4,Consultants,4,3,Greg,70000,active",
                 ""
               ],
               "\r\n"
             ) ==
               right_join(
                 db,
                 "employee",
                 "department",
                 &(&1["employee.department_id"] == &1["department.id"])
               )
               |> csv()
    end
  end

  describe "order_by/2" do
    setup :employee_data

    test "order by name", %{db: db} do
      assert "name,status\r\nGarth,active\r\nGreg,active\r\nJosh,active\r\nMichael,inactive\r\nRuth,inactive\r\n" ==
               db
               |> from("employee")
               |> order_by(&(&1["name"] < &2["name"]))
               |> select([:name, :status])
               |> csv()
    end

    test "order by status", %{db: db} do
      assert "name,status\r\nGarth,active\r\nGreg,active\r\nJosh,active\r\nMichael,inactive\r\nRuth,inactive\r\n" ==
               db
               |> from("employee")
               |> order_by(&(&1["status"] < &2["status"]))
               |> select([:name, :status])
               |> csv()
    end
  end

  describe "join/4 (inner_join/4)" do
    setup :employee_data

    test "join on join table", %{db: db} do
      # SELECT * FROM employee JOIN employee_club ON employee_club.a = employee.id JOIN club ON club.id = employee_club.b

      employee = from(db, "employee")
      employee_club = from(db, "employee_club")
      club = from(db, "club")

      result =
        employee
        |> join(employee_club, &(&1["employee_club.A"] == &1["employee.id"]))
        |> join(club, &(&1["employee_club.B"] == &1["club.id"]))
        |> select(["employee.name", "club.name"])
        |> order_by(&(&1["employee.name"] < &2["employee.name"]))
        |> csv()

      assert Enum.join(
               [
                 "employee.name,club.name",
                 "Greg,Asian Languages",
                 "Greg,Carbon Offset Club",
                 "Josh,Carbon Offset Club",
                 "Josh,Cat Lovers",
                 "Michael,Carbon Offset Club",
                 "Michael,Book Drive",
                 "Michael,House Builders",
                 "Michael,Cat Lovers",
                 "Ruth,Cat Lovers",
                 ""
               ],
               "\r\n"
             ) == result
    end
  end

  describe "where/3" do
    setup :employee_data

    test "employees members of cat lovers", ctx do
      assert %Table{
               name: nil,
               rows: [
                 %{"employee.id" => 1},
                 %{"employee.id" => 2},
                 %{"employee.id" => 4}
               ]
             } =
               ctx.db
               |> inner_join(
                 "employee",
                 "employee_club",
                 &(&1["employee_club.A"] == &1["employee.id"])
               )
               |> inner_join(from(ctx.db, "club"), &(&1["employee_club.B"] == &1["club.id"]))
               |> where(&(&1["club.name"] == "Cat Lovers"))
    end
  end

  describe "distinct" do
    setup :employee_data

    test "distinct(friends, [city, state])", %{db: db} do
      # +------+------------------+----------+
      # | id   | city             | state    |
      # |------+------------------+----------|
      # | 1    | Denver           | Colorado |
      # | 2    | Colorado Springs | Colorado |
      # | 3    | South Park       | Colorado |
      # | 4    | Corpus Christi   | Texas    |
      # | 5    | Houston          | Texas    |
      # | 6    | Denver           | Colorado |
      # | 7    | Corpus Christi   | Texas    |
      # +------+------------------+----------+
      friends =
        db
        |> create_table("friends")
        |> insert_into("friends", [
          %{id: 1, city: "Denver", state: "Colorado"},
          %{id: 2, city: "Colorado Springs", state: "Colorado"},
          %{id: 3, city: "South Park", state: "Colorado"},
          %{id: 4, city: "Corpus Christi", state: "Texas"},
          %{id: 5, city: "Houston", state: "Texas"},
          %{id: 6, city: "Denver", state: "Colorado"},
          %{id: 7, city: "Corpus Christi", state: "Texas"}
        ])
        |> from("friends")

      assert Enum.join(
               [
                 "city,state",
                 "Colorado Springs,Colorado",
                 "Corpus Christi,Texas",
                 "Denver,Colorado",
                 "Houston,Texas",
                 "South Park,Colorado",
                 ""
               ],
               "\r\n"
             ) ==
               distinct(friends, [:city, :state])
               |> csv()
    end

    test "distinct(book, [status])", %{db: db} do
      book =
        db
        |> create_table("book")
        |> insert_into("book", [
          %{id: 1, name: "The C Programming Language", status: "Checked Out"},
          %{id: 2, name: "SQL Fundamentals", status: "Checked Out"},
          %{id: 3, name: "The Magic Garden Explained", status: "Checked Out"},
          %{id: 4, name: "The Art of Computer Programming", status: "Available"},
          %{id: 5, name: "Design Patterns", status: "Available"},
          %{id: 6, name: "Compilers, ", status: "Missing"}
        ])
        |> from("book")

      # ┌───────────────┐
      # │    status     │
      # ├───────────────┤
      # │  Checked Out  │
      # │   Available   │
      # │    Missing    │
      # └───────────────┘

      assert %Table{
               name: "book",
               rows: [
                 %{"status" => "Available"},
                 %{"status" => "Checked Out"},
                 %{"status" => "Missing"}
               ],
               _select: ["status"]
             } ==
               book
               |> select(["status"])
               |> distinct(["status"])
    end

    test "distinct on join", %{db: db} do
      # Distinct can work on multiple columns
      #
      # This query joins on club and then does distinct on status and club.name which
      # leads to (active, Cat Lovers) and (inactive, Environmentalists) being condensed to
      # a single row
      #
      # SELECT distinct status, club.name, COUNT(*) AS count FROM employee
      # JOIN employee_club ON employee_club.a = employee.id
      # JOIN club ON club.id = employee_club.b
      # WHERE employee.salary > 150000
      # GROUP BY status, club.name

      # Result:
      # ┌─────────────────┬──────────────────────┬───────┐
      # │ employee.status │  club.name           │ count │
      # ├─────────────────┼──────────────────────┼───────┤
      # │     active      │      Cat Lovers      │   2   │
      # │    inactive     │  Environmentalists   │   1   │
      # │    inactive     │  Education for Kids  │   1   │
      # │     active      │    House Builders    │   1   │
      # │     active      │  Food for the Needy  │   1   │
      # │     active      │  Environmentalists   │   1   │
      # └─────────────────┴──────────────────────┴───────┘
      employee = from(db, "employee")
      club = from(db, "club")
      employee_club = from(db, "employee_club")

      assert Enum.join(
               [
                 "employee.status,club.name,count",
                 "inactive,House Builders,1",
                 "inactive,Carbon Offset Club,1",
                 "inactive,Book Drive,1",
                 "active,Carbon Offset Club,1",
                 "active,Asian Languages,1",
                 "inactive,Cat Lovers,2",
                 ""
               ],
               "\r\n"
             ) ==
               employee
               |> join(employee_club, &(&1["employee_club.A"] == &1["employee.id"]))
               |> join(club, &(&1["employee_club.B"] == &1["club.id"]))
               |> where(&(&1["employee.salary"] > 50000))
               |> group_by(["employee.status", "club.name"])
               |> count("club.name")
               |> select(["employee.status", "club.name", "COUNT(club.name)"], %{
                 "COUNT(club.name)" => "count"
               })
               |> distinct(["employee.status", "club.name", "count"])
               |> order_by(&(&1["count"] < &2["count"]))
               |> csv()
    end
  end

  defp cross_data(ctx) do
    db =
      ctx.db
      |> create_table("test1")
      |> insert_into("test1", %{c: "A"})
      |> insert_into("test1", %{c: "B"})
      |> create_table("test2")
      |> insert_into("test2", %{c: "1"})
      |> insert_into("test2", %{c: "2"})
      |> create_table("test3")
      |> insert_into("test3", %{c: "X"})
      |> insert_into("test3", %{c: "Y"})

    {:ok, db: db}
  end

  defp employee_data(ctx) do
    db =
      ctx.db
      |> create_table("employee")
      |> insert_into("employee", [
        %{id: 1, name: "Josh", department_id: 1, salary: 50000, status: "active"},
        %{id: 2, name: "Ruth", department_id: 2, salary: 60000, status: "inactive"},
        %{id: 3, name: "Greg", department_id: 4, salary: 70000, status: "active"},
        %{id: 4, name: "Michael", department_id: 3, salary: 80000, status: "inactive"},
        %{id: 5, name: "Garth", department_id: 2, salary: 35000, status: "active"}
      ])
      |> create_table("department")
      |> insert_into("department", [
        %{id: 1, name: "Sales"},
        %{id: 2, name: "Engineering"},
        %{id: 3, name: "Management"},
        %{id: 4, name: "Consultants"}
      ])
      |> create_table("club")
      |> insert_into("club", [
        %{id: 1, name: "Cat Lovers"},
        %{id: 2, name: "House Builders"},
        %{id: 3, name: "Book Drive"},
        %{id: 4, name: "Carbon Offset Club"},
        %{id: 5, name: "Asian Languages"},
        %{id: 6, name: "Weekly Potluck"}
      ])

      # join table for many-to-many relation between employee and club
      # employees can be in zero or more groups
      # groups can have zero or more employees
      |> create_table("employee_club")
      |> insert_into("employee_club", [
        %{A: 1, B: 1},
        %{A: 1, B: 4},
        %{A: 2, B: 1},
        %{A: 3, B: 4},
        %{A: 3, B: 5},
        %{A: 4, B: 1},
        %{A: 4, B: 2},
        %{A: 4, B: 3},
        %{A: 4, B: 4}
      ])

    {:ok, db: db}
  end

  def games_data(ctx) do
    db =
      ctx.db
      # +------+--------+
      # | id   | name   |
      # |------+--------|
      # | 1    | Josh   |
      # | 2    | Ruth   |
      # | 3    | Carl   |
      # +------+--------+
      |> create_table("player")
      |> insert_into("player", [
        %{id: 1, name: "Josh"},
        %{id: 2, name: "Ruth"},
        %{id: 3, name: "Carl"}
      ])
      # +-------------+------------+----------+------------------+
      # | player_id   | type       | result   | length_minutes   |
      # |-------------+------------+----------+------------------|
      # | 1           | Chess      | Win      | 23.5             |
      # | 1           | Chess      | Loss     | 26.5             |
      # | 2           | Checkers   | Loss     | 6.5              |
      # | 2           | Dominos    | Loss     | 9.1              |
      # | 1           | Battleship | Win      | 27.9             |
      # +-------------+------------+----------+------------------+
      |> create_table("games")
      |> insert_into("games", [
        %{player_id: 1, type: "Chess", result: "Win", length_minutes: 23.5},
        %{player_id: 1, type: "Chess", result: "Loss", length_minutes: 26.5},
        %{player_id: 2, type: "Checkers", result: "Loss", length_minutes: 6.5},
        %{player_id: 2, type: "Dominos", result: "Loss", length_minutes: 9.1},
        %{player_id: 1, type: "Battleship", result: "Win", length_minutes: 27.9}
      ])

    {:ok, db: db}
  end
end
