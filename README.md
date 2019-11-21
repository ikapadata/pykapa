# pykapa
This a free and open source python script to monitor, manage, and clean collected data. It is intended to provide (a) affordable quality assurance tools in resource-constrained environments and (b) lower the barrier of programming level needed to automate scripts. Google Sheets is used as the programming interface, so the minimum requirements are knowledge of Google Sheets formulas and XLS forms. An added bonus is the transparency offered by posting quality issues associated with data and progress reports on Slack. Therefore, all stakeholders are able to monitor, track and follow up on issues in real time. A user only needs to provide their [slack bot token](https://slack.com/intl/en-za/help/articles/215770388-create-and-regenerate-api-tokens#bot-user-tokens). If incentives are associated with the research, then airtime, SMS or data bundle incentives are awarded to respondents.  

- [Google Sheets](https://docs.google.com) [Free] - create survey, set quality control and incentive parameters, edit and clean data.
- [Slack](https://slack.com) [Free] - post quality control issues and progress reports, transparent and effective communication and collaboration between stakeholders.
- [Data Studio](https://datastudio.google.com) [Free] - data visualisation.
- [surveyCTO](https://www.surveycto.com) [Paid] - Android application, data collection and storage.
- [SimControl](https://new.simcontrol.co.za/) or [flickswitch](https://www.flickswitch.co.za) (Optional) [Paid] - Airtime, SMS and Data bundles distribution. 
- [Dropbox](http://dropbox.com)(Optional) [Free] - online backup of collected and clean data.


# 1. Google Sheet Setup
This section provides an overview of the necessary and optional sheets needed to monitor and clean data or send incentives. View the [template](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit) for guidance.

## 1. 1. **Necessary Sheets**
It is compulsory to have the following worksheets for pykapa to monitor data collection and post messages on Slack.

### [survey](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit)
This worksheet is very important as it is the interface for creating questions and structure for your questionnaire. You will have to add the **dashboard_state** column and set the values to TRUE for fields that you want visualise on Data Studio.

### [choices](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1259247300)
You need this worksheet to be able to pull the label for select and select_multiple fields using the `jr:choice-name(${field}, '${field}')` such that your messages on Slack are readable.

### [settings](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1265829571)
You need this worksheet since it contains the **form_id** needed to download the data from surveyCTO.

### [messages](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1628036173)
This worksheet is the interface to program the quality control conditions and resulting messages when they are satisfied. You have to create this worksheet and its columns. Create the **channel_id**, **message_relevance**, ***message_label**,	**dashboard_state**,and **name** columns. Set the dashboard_sate values to TRUE for alerts you wish to visualise on Data Studio.

### [messages_settings](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=2118996547)
In this worksheet you assign headers and slack channels for messages to to be posted. You have to create this worksheet and its columns. Create the **channel_id**, **channel_name**, **message_header**, and **messenger** columns.

## 1.2. **Optional Sheets**
Create the following sheets only if you will use them.

### [corrections](https://docs.google.com/spreadsheets/d/1WUm39fSxk9gigXw5SDbmYiRtg68CW0EMRVnZ1rNemdI/edit#gid=275124491)
This worksheet allows you to clean data by correcting mistakes made during data collection. You can correct multiple field-values per row. It is limited to `replace` and `drop`, where you replace values or drop observations respectively. You have to create this worksheet and its columns. Create the **action**, **relevance**,	**correction**,	**comment** columns. 

### [incentives_settings](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=1366715473)
This worksheet allows you incentivise respondents with airtime, data bundles or sms bundles. You have to create this worksheet and its columns. Create the **incentive_type**, **amount**, **contact**, **network**, **recharge_count**, **flickswitch_api_key**, and **messenger** columns.

### [to_dropbox](https://docs.google.com/spreadsheets/d/1yZfpCAV1BHkHBwfncnvZebFN0xRcUyOa6j6gcwkBlxk/edit#gid=86807683)
This worksheet allows you to back your data on [Dropbox](http://dropbox.com). You have to create this worksheet and its columns. It consist of only two colums, **dropbox_dir** and **dropbox_token**. View [generate dropbox token](https://blogs.dropbox.com/developers/2014/05/generate-an-access-token-for-your-own-account/) to follow the instruction on how to obtain an access token.

## 1.3. **Programmatically Created Sheets**
The following worksheets are created automatically when the program is running. You must not create any worksheet with the following names.

### [dashboard](https://docs.google.com/spreadsheets/d/1J7vr1fY8PlsXcAlCewMDBbMsxdHICZPR7CoPby-MYBs/edit#gid=951543888)
This worksheet is created automatically if the survey or messages worksheets have the **dashboard_state** column with TRUE values. You can then use the data in this worksheet to create dashboards using Data Studio.

# 2. Run Python Script

If you running the script on a terminal it is advised to create tmux environments. So, you can use the same script for multiple forms - it's useful for big projects. Create a new **tmux** session. The following example shows a new **tmux** session called _new_session_name_ .
View [tmux-commands](https://gist.github.com/MohamedAlaa/2961058) for more information on tmux installation and commands.

    $ tmux new -s new_session_name

After creating a session for your form navigate to the folder containing the python script and run it as follows.

    $ cd pykapa/pykapa
    $ python3 qcMessenger.py

You will then be prompted to enter the google sheet link, surveyCTO credentials, slack bot token, slack channel to post error messages that the script encounters during execution.

Thereafter, you can exit the session to have it running in the backend without interruption. To exit the session do the following hold the keys `control + b` thereafter click `d`. Once you have exited you can create another session for another form or exit the terminal.

To attach to the created session type the following, in this case you're attaching to `new_session_name`.

    $ tmux a -t new_session_name

At the end of a project you will have to end the session by typing the following outside the session.

    $ tmux kill-session -t new_session_name


