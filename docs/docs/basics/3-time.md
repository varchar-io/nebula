---
layout: default
---

# Time 
column, value and unit

Internally - Nebula uses a reserved time column `__time__` to represent time point for every single record.
It uses unix time UTC time value in seconds.

## Local friendly time in the future
In the future, we will enable user to see converted local time on its UI.

## Time are supported mostly by javascript built-in Date object.
So all user input from web or other client will be converted by `time.seconds` method.

## Various forms of time supported
The time range start and end value can be any type of below format as long as it's valid

- milliseconds: unix time in UTC milliseconds
- string format: such as `2020-07-07` or `2020-07-07 13:12:11`, milliseconds supported but will be rounded as Nebula supports time granularity at seconds level.
- common literals, like:
  - now: represents right now
  - -{x}{u}: represents some time ago including minutes, hours, days and weeks. These examples are all valid:
    - -3days: 3 days ago
    - -15m: 15 minutes ago
    - -2wk: 2 weeks ago
    - -4h: 4 hours ago.

These units are supported right now as defined in code `time.js`:
-   minute: S_MIN,
-   minutes: S_MIN,
-   min: S_MIN,
-   mins: S_MIN,
-   m: S_MIN,
-   hour: S_HR,
-   hours: S_HR,
-   hr: S_HR,
-   hrs: S_HR,
-   h: S_HR,
-   day: S_DAY,
-   days: S_DAY,
-   d: S_DAY,
-   week: S_WEEK,
-   weeks: S_WEEK,
-   wk: S_WEEK,
-   wks: S_WEEK,
-   w: S_WEEK
