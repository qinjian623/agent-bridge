> English | [中文](zh/privacy.md)

# Privacy when preparing a public repository

The public source tree uses generic examples. Personal home directories, real host addresses, session/process identifiers, private project names, and private report locations do not belong in committed examples. Public project repository URLs and third-party product names are retained. Synthetic IDs, loopback addresses, documented default directories, protocol fields, and test values can remain where their purpose is clear.

Historical verification notes distinguish observed results, failed checks, and work that was not executed. Replacing an identifier with a descriptive placeholder does not turn a failed check into a pass. Private reports referenced by these notes are not included in the repository.

## Check files and history separately

Run the content check against the files intended for publication:

```bash
python3 scripts/check_public_content.py
```

It reports locations and categories without printing matched values. It detects common formats, not every private project name, partial identifier, deployment note, image, or credential. Review its output and inspect the source tree manually. Keep replacement rules, original identifiers, raw reports, and before/after maps outside the public repository.

A clean working tree does not imply a clean Git history. Review all branch/tag names, commit and tag messages, author/committer/tagger metadata, and historical file contents. Reflogs, old history backups, linked worktree heads, runtime snapshot/workspace refs, and unreachable objects may retain earlier data even when they are absent from the current branch.

## Prepare a separate candidate

Perform history rewriting in a private, independent mirror without object hardlinks to the original repository. Use a maintained tool such as `git-filter-repo`; do not edit the shared repository's refs, object files, index, or worktree registration. Limit the public candidate to intentionally retained development branches and release tags. Runtime recovery refs and old-history backup refs stay private.

When identities are anonymized, names such as `CLI Agent Bridge contributor (anonymized)` and addresses under `example.invalid` are explicit placeholders, not verified real-world attribution. Preserve third-party attribution and licensing notices. Do not infer missing contributor permission from a placeholder.

History rewriting changes commit IDs and annotated tag IDs. Preserve the private before/after mapping, check parent relationships and retained tag names, and record whether signatures existed. Rewritten signatures cannot remain valid; signed releases require a deliberate new signing and publication step. Version labels alone do not prove that an old release artifact matches the rewritten source.

After rewriting, check every retained ref and its historical objects, confirm the mirror has no source-repository remote or shared object hardlinks, and inspect it with `git fsck`. Apply the final reviewed source tree only after it has stopped changing, then repeat the file/content checks. A sanitized historical snapshot alone does not include later source fixes or documentation.

If historical removal cannot be verified with reasonable confidence, prepare a fresh public history from reviewed current files instead. Retain original history privately. Creating a local candidate does not publish it, change an existing remote, or authorize a force push.

## Limits

Removing data from this candidate cannot erase other clones, remote caches, forks, release assets, backups, or already published messages. Exposed credentials need separate revocation. Do not publish runtime databases, authentication state, environment files containing secrets, raw conversations, or screen recordings, even when their filenames look harmless.
