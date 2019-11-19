# pykapa
This package leverages on ODK platforms in a mission to provide affordable research tools to researchers for data collection, monitoring, and cleaning. An integration between pykapa and Slack messenger is used to post quality issue alerts, and progress reports on specific channels, which researchers can access to monitor data collection and instantly follow up quality issues and correct them timeuosly. An added bonus is the transparency that comes with instant messaging-based communication as relevant stakeholders can monitor all communication in real time.

The package reads an XLS form (Google Sheet) quality control conditions on the collected data and posts messages to relevant slack channels when the conditions are satisfied. if incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.  

This version only works for XLS forms created on Google Sheets, data collected and stored via surveyCTO, incentives distribiuted via flickswitch or SimControl.
