#!/usr/bin/env python
#### EC2InstanceScheduler.py --- Start/stop EC2 instances based on crontab-like tags

### Copyright (c) 2017, Matthew X. Economou <xenophon@irtnog.org>
###
### Permission to use, copy, modify, and/or distribute this software
### for any purpose with or without fee is hereby granted, provided
### that the above copyright notice and this permission notice appear
### in all copies.
###
### THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
### WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
### WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
### AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
### CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
### LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
### NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
### CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

### This file installs an AWS Lambda function that handles scheduled
### CloudWatch Events.  When triggered it scans all regions for EC2
### instances with crontab-like tag-values that specify when the
### instance should start/stop, and then acts accordingly.  The key
### words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
### "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in
### this document are to be interpreted as described in RFC 2119,
### https://tools.ietf.org/html/rfc2119.  The key words "MUST (BUT WE
### KNOW YOU WON'T)", "SHOULD CONSIDER", "REALLY SHOULD NOT", "OUGHT
### TO", "WOULD PROBABLY", "MAY WISH TO", "COULD", "POSSIBLE", and
### "MIGHT" in this document are to be interpreted as described in RFC
### 6919, https://tools.ietf.org/html/rfc6919.  The keywords "DANGER",
### "WARNING", and "CAUTION" in this document are to be interpreted as
### described in OSHA 1910.145,
### https://www.osha.gov/pls/oshaweb/owadisp.show_document?p_table=standards&p_id=9794.

import boto3
from string import translate, maketrans
from datetime import datetime

## Connect using the default session.
client = boto3.client('sts')

## Prefix the tags used by this script with the account ID.
tag_prefix = client.get_caller_identity()['Account']
auto_start_tag =    '{}:auto-start'   .format(tag_prefix)
auto_stop_tag =     '{}:auto-stop'    .format(tag_prefix)

## Represent schedule fields as ranges of numbers.  This wraps range()
## so that the stopping point gets included in the range, i.e.,
## `[start, stop]` instead of `[start, stop)`, and so that its
## arguments get converted to integers automatically.
def _range(start, stop=None, step=1):
    if stop == None:
        stop = start
    return range(int(start), int(stop) + 1, int(step))

## Use string translation tables and hashes when parsing schedule
## fields.
T = maketrans('-/', '  ')
months = {
    'jan': '1', 'feb': '2', 'mar': '3',
    'apr': '4', 'may': '5', 'jun': '6',
    'jul': '7', 'aug': '8', 'sep': '9',
    'oct': '10', 'nov': '11', 'dec': '12',
}
weekdays = {
    'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3',
    'thu': '4', 'fri': '5', 'sat': '6',
}

## Replace instances of the string '*' with the value of max, and then
## split the field into a list of number ranges.  Use string.split()
## with string.translate() to parse the number range specifiers.
## Return a flattened list.
def _parse_field(field, max):
    f = field.replace('*', max).split(',')
    l = [ _range(*translate(f_element, T).split(' '))
          for f_element in f ]
    return [ item for sublist in l for item in sublist ]

## Parse each field in the schedule.
def _parse_schedule(schedule):
    schedule = schedule.lower() # case canonicalization

    ## specials
    if schedule in ['@yearly', '@annually']:
        return _parse_schedule('0 0 1 1 *')
    if schedule in ['@monthly']:
        return _parse_schedule('0 0 1 * *')
    if schedule in ['@weekly']:
        return _parse_schedule('0 0 * * 0')
    if schedule in ['@daily', '@midnight']:
        return _parse_schedule('0 0 * * *')
    if schedule in ['@hourly']:
        return _parse_schedule('0 * * * *')
    if schedule in ['@every_minute']:
        return _parse_schedule('*/1 * * * *')

    ## break the schedule into individual fields
    (minute, hour, day, month, weekday) = schedule.split(' ')

    ## convert month, day name abbreviations into numbers
    month = months[month] if month in months else month
    weekday = weekdays[weekday] if weekday in weekdays else weekday

    ## parse each field
    return {
        'minute':  _parse_field(minute,  '0-59'),
        'hour':    _parse_field(hour,    '0-23'),
        'day':     _parse_field(day,     '1-31'),
        'month':   _parse_field(month,   '1-12'),
        'weekday': _parse_field(weekday, '0-6'),
    }

## Compare the provided timestamp to the schedule.
def _scheduled(t, s):
    schedule = _parse_schedule(s)
    return (
        (t.minute in schedule['minute']) and
        (t.hour in schedule['hour']) and
        (t.day in schedule['day']) and
        (t.month in schedule['month']) and
        (t.isoweekday() in schedule['weekday']
         if 7 in schedule['weekday'] else
         t.weekday() in schedule['weekday'])
    )

def lambda_handler(event, context):
    now = datetime.utcnow()

    for region in boto3.DEFAULT_SESSION.get_available_regions('ec2'):
        ec2 = boto3.resource('ec2', region_name=region)

        ## Loop over instances with auto-start tags, and start them if
        ## scheduled to do so.
        for instance in ec2.instances.filter(
                Filters=[{
                    'Name': 'tag-key',
                    'Values': [
                        auto_start_tag
                    ]}]):
            s = [ tag['Value']
                  for tag in instance.tags
                  if tag['Key'] == auto_start_tag ] [0]
            if _scheduled(now, s):
                instance.start()

        ## Loop over instances with auto-stop tags, and stop them if
        ## scheduled to do so.
        for instance in ec2.instances.filter(
                Filters=[{
                    'Name': 'tag-key',
                    'Values': [
                        auto_stop_tag
                    ]}]):
            s = [ tag['Value']
                  for tag in instance.tags
                  if tag['Key'] == auto_stop_tag ] [0]
            if _scheduled(now, s):
                instance.stop()

    return

#### EC2InstanceScheduler.py ends here.
