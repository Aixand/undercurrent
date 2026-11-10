# Write clear technical English

Use an **STE-based software writing profile**. Formal ASD-STE100 compliance
requires review against its writing rules and controlled vocabulary. This guide
alone does not establish that compliance. Technical names and verbs can remain
precise; see the [official sources](references.md#language-and-documentation-design).

## State the action or fact first

Use active voice and identify the component responsible for an action. Address
the reader as "you" when needed. Begin instructions with a verb.

| Avoid | Prefer |
| --- | --- |
| In order to perform configuration validation, it is necessary to execute the following command. | Run the configuration validation command. |
| The request is processed. | The service validates the request. |
| Simply restart the service. | Restart the service. |
| The operation was successful. | The command exits zero and prints the created record ID. |

Put conditions before the actions they control: "If the connection closes,
inspect the saved receipt before retrying." Preserve the actual failure contract.

## Keep sentences and paragraphs focused

Give one main instruction per procedural sentence. Aim for at most 20 words in
instructions and 25 in descriptive sentences. These are review targets; preserve
meaning and exact technical names. The bundled checker does not assess STE.

Use a short paragraph for one idea. Use lists for ordered actions or parallel
items. Use tables for values and comparisons. Split long noun strings into clear
phrases. Remove repeated summaries, marketing language, idioms, and filler.

## Use terms consistently

Prefer familiar words: use, start, stop, create, read, write, and check. Use one
term for one concept. Define unfamiliar terms when needed and link to the owning
glossary when one exists. Related products may have different meanings for
"session" or "run"; identify the relevant product rather than forcing equivalence.

Keep command names, identifiers, units, literal output, UI labels, and quoted
errors exact. Use American English for new prose. Preserve external names.

## Format for scanning

Use one H1 per Markdown page and sentence case for headings. Do not skip heading
levels. Name task headings with an action, such as "Restore a backup". Reference
headings can name the contract being described.

Use fenced code blocks with a language. Do not include a shell prompt in commands
readers should copy. Identify placeholders before their first command. Separate
sample output from commands and explain only meaningful variable output.

Use descriptive link labels and relative links within a repository. Use a working
published URL for another repository. Avoid machine-specific paths in published
instructions. Standard home-directory examples are acceptable when they reflect
actual product behavior.

## Keep examples maintainable

Prefer a complete runnable example stored once. Link to it from other pages or
generate excerpts from it. Mark an excerpt that needs surrounding code. Comments
should explain a constraint or decision, rather than repeat an obvious statement.

[Choose a visual](diagrams.md) · [Required standard](standard.md)
