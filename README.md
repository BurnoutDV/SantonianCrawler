# Santonian Crawler

Fan Project regarding the game [GTFO](https://gtfothegame.com/)

This is, currently, a command line application that leverages the [Santonian Industries](https://santonianindustries.com/) API that was probably not meant to be an API. As of now the virtual console on the websites calls a backend that can be called directly.

**DISCLAIMER: do not abuse the abilities of this program and stay local as much as possible**

Its original purpose was to crawl the entire database provided by the website and periodically check for changes, both in overall scope (read: new entries) but also for changes in the existing log files to divine some clues out of those changes. All data is saved in a local sqlite database

There is also a cli interface and a planned TUI.



## Some CMD features

Listing of logs in a local database to provide quick access without the internet

![archive_list](README/archive_list.png)

Complex listing of all logs with out without filters, order and pagination

![data_list](README/data_list.png)

Tagging of individual logs and automatically matching of dates via regex:

![tagging](README/tagging.png)

Sorting by individual columns and date tags:

![time_sort](README/time_sort.png)



## Missing Features

There is quite a lot i yet want to build

* cmd: Ability to trigger partial download by cmd
* cmd: Ability to trigger Tabula Rasa download by cmd
* cli arguments to allow cron job usage for recurring checks
* Text User Interface, maybe with [Textual](https://github.com/Textualize/textual)?
* cmd: filter by content
* cmd: proper datatable cmd interface, it messes up sometimes
* doc texts, expecially in the cmd side but also in the db backend
* properly integrating the santonian download codes in the rest

### Development Notes

This pile of spaghetti was being developed in PyCharm and i used some plugins to make it *better* for me, of note are two plugins:

* Rainbow Brackets
* Comments Highlighter

Those bring some color in the code and make the comments a bit more granular:

![dev_colors](README/dev_colors.png)
