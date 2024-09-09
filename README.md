# SqltoyElixir

This is an implementation of Joshua Weinberg's [SQLToy][sqltoy] in Elixir,
implemented because I thought it was a neat exercise. It is mostly translated
from the JavaScript implementation, but the [wiki][wiki] is fascinating and
explains a lot more of what's going on.

While `mix.exs` declares Elixir 1.17, most of the functionality here should be
compatible several versions.

## Code Variants

The version of the code at tag [reduce][reduce] uses `Enum.reduce/3` for the
complex implementation bits and has 303 lines of code for the core
implementation. The version of the code at tag [comprehension][comprehension]
uses list comprehensions and has 278 lines of code. There are a couple of minor
code improvements at the `comprehension` tag that would shorten the `reduce`
implementation by about 15 lines.

In both versions, there are an additional 170 lines of code for CSV and table
formatting to keep it at zero dependencies other than Elixir. The example code
in Joshua's implementation (`src/samples`) have been turned into 654 lines of
test code.

As with Joshua's version, this is not optimized nor robust. It is a learning
experiment. There are some things present which make all of the stored rows use
binary keys, converting maps as required (atom keys are used for internal
implementation details).

### Conclusions

In general, I _personally_ find that `Enum.reduce/3` is easier to reason about,
but reviewing the changes in the reduce code suggest that _some_ of the list
comprehensions are

## Installation

No.

[sqltoy]: https://github.com/weinberg/SQLToy
[wiki]: https://github.com/weinberg/SQLToy/wiki
[reduce]: https://github.com/halostatue/sqltoy_elixir/tree/reduce
[comprehension]: https://github.com/halostatue/sqltoy_elixir/tree/comprehension
