# Source revisions for the series

A worktree is a separate checkout that shares Git history. This worktree starts
at `0e5dd3528b9423dda9e18e99b131ebe61fbc82cb`. A commit records a version of
the project. Every commit below exists in its local Git history.

When we wrote this draft, recent accepted changes existed only locally. Their
chapter references point here. GitHub links for those commits need an upload
to the remote repository first.

Run the commands from the root of this repository. `git show <revision>:main.go`
reads the source at that commit. `git diff <revision>^ <revision> --
main.go` shows the change from its parent commit. These commands do not modify
files.

## c27aa54

This commit adds the first Go implementation. Its full identifier is
`c27aa549c3182866d56d71f1537ece3b900bde33`. The first command displays its
source. The second displays its README.

```sh
git show c27aa54:main.go
git show c27aa54:README.md
```

The source sets the chunk size to 32 MiB. The README contains the old Mac
timing table and the `benchstat` excerpt. This Git history contains no earlier
naive Go version.

## 9233101

This commit repairs array bounds for short rows. Its full identifier is
`9233101acd7fa232d772e9736b28d8b74ec13fe3`. This command shows the code and
README changes from its parent commit.

```sh
git diff 9233101^ 9233101 -- main.go README.md
```

## a1cd728

This commit changes iteration, merging, rounding, and chunk size. Its full
identifier is `a1cd72836776d8f42f8322274583e39c4a64be7b`. This command shows
the source and test changes from its parent commit.

```sh
git diff a1cd728^ a1cd728 -- main.go main_test.go
```

## 83098c9

This commit passes table descriptors by value. It also retains a standard-map
experiment. Its full identifier is `83098c93f878c16c490894a63f6feff9675e3e86`.
The first command shows the source changes. The second displays the experiment
file.

```sh
git diff 83098c9^ 83098c9 -- main.go
git show 83098c9:main_stdmap.go.test
```

## 5a77773

This commit replaces `math.Round` with integer rounding in `main.go`.
Its full identifier is `5a7777364dc7ea36f562a3ad05b557b9bfce862a`.
This command shows the exact change from its parent commit.

```sh
git diff 5a77773^ 5a77773 -- main.go
```

## 84e642c

This commit adds SWAR delimiter scanning. SWAR compares several bytes inside
one integer. Its full identifier is `84e642c4fd527e83e161f0b259c04e7c5be5cc7c`.
This command shows the code, test, and experiment-log changes.

```sh
git diff 84e642c^ 84e642c -- main.go main_test.go EXPERIMENTS.md
```

## f8a90a5

This commit adds the 32K station table with split lookup. Its full identifier
is `f8a90a532e4f6ac4f6c66cc9a86f4e28c52c5c03`. Its parent, `c890897`,
contains the old production table. The first command displays the new source.
The second shows the changes and tests.

```sh
git show f8a90a5:main.go
git diff f8a90a5^ f8a90a5 -- main.go main_test.go hotpath_test.go
```

## 51e20a1

This commit adds parser-word reuse. Its full identifier is
`51e20a133a3aadecd947e3e1248676899fc9e9bc`. We measured it against its
parent, `f8a90a5`. The first command displays the source. The second shows the
changes and tests.

```sh
git show 51e20a1:main.go
git diff 51e20a1^ 51e20a1 -- main.go parser_word_test.go
```

## ef7418a

This commit adds bounded temperature-word decoding. Its full identifier is
`ef7418a8be7fa57a02298fb49acbbfc3450b35e5`. We measured it against its
parent, `51e20a1`. The first command displays the source. The second shows the
changes and tests.

```sh
git show ef7418a:main.go
git diff ef7418a^ ef7418a -- main.go temperature_word_test.go
```

## 57591e7

This commit adds bounded buffer reuse. Its full identifier is
`57591e78ed9eb68626a5078840576c32f2d0a000`. We measured it against its
parent, `ef7418a`. The first command displays the source. The second shows the
changes and tests.

```sh
git show 57591e7:main.go
git diff 57591e7^ 57591e7 -- main.go buffer_pool_test.go
```

## 0e5dd35

This commit adds pooled AVX2 scanning. Its full identifier is
`0e5dd3528b9423dda9e18e99b131ebe61fbc82cb`. Its immediate parent,
`d576ea0`, updates documentation. We measured its code against `57591e7`.

The first command displays the vector kernel. The second compares the code
with `57591e7`. It excludes the intervening documentation update.

```sh
git show 0e5dd35:mask_simd_amd64.go
git diff 57591e7 0e5dd35 -- main.go mask_simd_amd64.go mask_fallback.go prepare_go-lunemec.sh
```

The chapter's final comparisons use the original, fixed production binary.
The controls with matching build flags are separate comparisons. Their results
remain in the full experiment archive.

## Full branch chronology

This command lists commits in parent order. It includes dates and commit
subjects. This order supplies the sequence of chapters.

```sh
git log --reverse --format='%h %ad %s' --date=short c923467..0e5dd35
```

Parts 1 and 2 describe the intervening benchmark work and correctness repairs.
Documentation-only commits do not change the parser. The series does not
attribute parser gains to those commits.
