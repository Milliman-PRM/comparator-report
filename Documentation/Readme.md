### OBJECTIVE:
 * House documentation intended for non-developer audiences (internal or external to Milliman).

### DEVELOPER NOTES:

This module should include such items as:
  * Release Notes (likely targeted at internal Milliman staff)
  * User Guides (targeted at end users)

It is encouraged for these documents to live in something as close to plain text as possible.  This makes them much easier to work with in the source control branch web.  Compilation scripts should be maintained (and ran automatically upon promotion). 

Currently, the user guides are implemented through Markdown documents that are compiled via [pandoc](http://pandoc.org/). Pandoc follows a slightly altered version of John Gruber’s Markdown syntax. Frequent editors of user guides should familiarize themselves with [pandoc's markdown](http://pandoc.org/README.html#pandocs-markdown) syntax. In particular, the section about inline images, because our user guides often differ from the default implementation. For example, when using MarkdownPad, a backslash is shown after an image, but this backslash is necessary for implementing the image through pandoc.

The user guides follow the Milliman Editorial Style Guide which can be found at [Milliman Editorial Style Guide](https://us-intranet.milliman.com/corppub/marcom/Marcom%20Forms%20and%20Policies/Guidelines%20-%20Milliman%20Editorial%20Style%20Guide.pdf). For example, the user guides practice using the oxford comma as stated in the style guide.
