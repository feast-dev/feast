# Feast RFCs

[Feast RFCs]: #feast-rfcs

Many changes, including bug fixes and documentation improvements can be
implemented and reviewed via the normal GitHub pull request workflow. 
However, any substantial changes should be put through a design process and
produce a concensus.

The "RFC" (request for comments) process is intended to provide a consistent
and controlled path for new features to enter the project.

## What the process is
[What the process is]: #what-the-process-is

In short, to get a major feature added to Feast, one must first get the RFC
merged into the RFC repository as a markdown file. At that point the RFC is
"active" and may be implemented with the goal of eventual inclusion into Feast.

  - Clone the `feast` repository.
  - Copy `rfc/0000-template.md` to `rfc/0000-my-feature.md` (where "my-feature" is
    descriptive. don't assign an RFC number yet).
  - Fill in the RFC. Put care into the details: RFCs that do not present
    convincing motivation, demonstrate understanding of the impact of the
    design, or are disingenuous about the drawbacks or alternatives tend to be
    poorly-received.
  - Submit a pull request. As a pull request the RFC will receive design
    feedback from the larger community, and the author should be prepared to
    revise it in response.
  - Build consensus and integrate feedback. RFCs that have broad support are
    much more likely to make progress than those that don't receive any
    comments. Feel free to reach out to the RFC assignee in particular to get
    help identifying stakeholders and obstacles.
  - The core team will discuss the RFC pull request, as much as possible in the
    comment thread of the pull request itself. Offline discussion will be
    summarized on the pull request comment thread.
  - Once the consensus is reached, the approvers will merge in the pull request
    after which the implementation phase begins.

## Attribution

This process and template is based on [Rust RFCs](https://github.com/rust-lang/rfcs).