# pykapa
This package leverages on ODK platforms in a mission to provide affordable research tools to researchers for data collection, monitoring, and cleaning. An integration between pykapa and Slack messenger is used to post quality issue alerts, and progress reports on specific channels, which researchers can access to monitor data collection and instantly follow up on quality issues and correct them timeuosly. An added bonus is the transparency that comes with instant messaging-based communication as relevant stakeholders can monitor all communication in real time.

The package reads an XLS form for quality control and data cleaning conditions on the collected data, then posts messages to relevant slack channels when the conditions are satisfied and saves the raw and clean data files. if incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.  

This version only works for XLS forms created on [Google Sheets](https://docs.google.com), data collected and stored via [surveyCTO](https://www.surveycto.com), incentives distribiuted via [flickswitch](https://www.flickswitch.co.za) or [SimControl](https://new.simcontrol.co.za/), dashboards created on [Data Studio](https://datastudio.google.com) for visualising progress and performance of enumerators, and post messages on [Slack](https://slack.com).

# 1. Google Sheet Setup
This section provides an overview of the necessary and optional sheets needed to monitor and clean data or send incentives. View the [template](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit) for guidance.

## 1. 1. **Necessary Sheets**

The following worksheets in the XLS form are necessary to monitor data collection and cleaning data.

### [survey](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit)

This worksheet is pre-existing in the XLS form template. This worksheet is very important as it is the interface creating questions and structure for your questionnaire. You will have to add the **dashboard_state** column and set the values to TRUE for fields that you want to use to create your dashboard.

### [choices](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1259247300)

This worksheet is pre-existing in the XLS form template. You need this worksheet to be able to pull the label for select and select_multiple fields using the `jr:choice-name(${field}, '${field}')` such that your messages on Slack are readable.

### [settings](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1265829571)

This worksheet is pre-existing in the XLS form template. You need this worksheet since it contains the **form_id** needed to download the data from surveyCTO.

### [messages](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1628036173)
This worksheet is the interface to program the quality control conditions and resulting messages when they are satisfied. You have to create this worksheet and its columns. Create the **channel_id, message_relevance, message_label,	dashboard_state,** and **name** columns.
