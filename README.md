# pykapa
This package leverages on ODK platforms in a mission to provide affordable research tools to researchers for data collection, monitoring, and cleaning. An integration between pykapa and Slack messenger is used to post quality issue alerts, and progress reports on specific channels, which researchers can access to monitor data collection and instantly follow up on quality issues and correct them timeuosly. An added bonus is the transparency that comes with instant messaging-based communication as relevant stakeholders can monitor all communication in real time.

The package reads an XLS form (Google Sheet) quality control conditions on the collected data and posts messages to relevant slack channels when the conditions are satisfied. if incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.  

This version only works for XLS forms created on Google Sheets, data collected and stored via surveyCTO, incentives distribiuted via flickswitch or SimControl.

# 1. Google Sheet Setup

## 1. 1. **Necessary Sheets**

The following sheets in the survey form are necessary for alerts to show on the [ikapadata.slack.com](http://ikapadata.slack.com) slack workspace. 

- `survey`

    This sheet is pre-existing in the survey form uploaded to [ikapadata.surveycto.com](http://ikapadata.surveycto.com). You will have to add the **dashboard_state** column and set the values to TRUE for names that you want to appear in the dashboard worksheet.

- `choices`

    This sheet is pre-existing in the survey form uploaded to [ikapadata.surveycto.com](http://ikapadata.surveycto.com). You need this worksheet to be able to pull the label for select  and select_multiple variables using the jr:choice-name(${variable}, '${varaible}').

- `settings`

    This sheet is pre-existing in the survey form uploaded to [ikapadata.surveycto.com](http://ikapadata.surveycto.com). You need this since it contains the **form_id** needed to download the data from [ikapadata.surveycto.com](http://ikapadata.surveycto.com).
