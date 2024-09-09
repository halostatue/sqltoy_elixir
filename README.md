# SqltoyElixir

This is an implementation of Joshua Weinberg's [SQLToy][sqltoy] in Elixir,
implemented because I thought it was a neat exercise. It is mostly translated
from the JavaScript implementation, but the [wiki][wiki] is fascinating and
explains a lot more of what's going on.

It has 303 lines of code for the core implementation and additional 170 lines of
code for CSV and table formatting to keep it at zero dependencies other than
Elixir. The examples in Joshua's code have been turned into 654 lines of test
and setup code.

While `mix.exs` declares Elixir 1.17, most of the functionality here should be
compatible several versions.

## Code Variants

The code at tag [reduce][reduce] uses `Enum.reduce/3` for its more complex
implementation pieces (I am generally more familiar with `Enum.reduce/3`), where
the head version (tagged [comprehension][comprehension]) uses list
comprehensions instead as an attempt to write more readable code for these
deeply nested behaviours.

As with Joshua's version, this is not optimized nor robust. It is a learning
experiment. There are some things present which make all of the stored rows use
binary keys, converting maps as required (atom keys are used for internal
implementation details).

## Installation

Don't. Explore it, but this is not something that should be published.

[sqltoy]: https://github.com/weinberg/SQLToy
[wiki]: https://github.com/weinberg/SQLToy/wiki
[reduce]: xx
[comprehension]: yy
