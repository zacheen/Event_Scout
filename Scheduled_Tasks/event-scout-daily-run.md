---
name: event-scout-daily-run
description: Runs the Event Scout local run in Event_Scout every day at 3 PM, which sends real mail, and checks the output for logic violations or new issues
---

Read D:\dont_move\git_save\Event_Scout\SCHEDULED_RUN.md, then follow the steps in that file exactly.

That file is the only source of rules for this task. Do not act from memory or guesswork, and do not add rules here.

If that file cannot be read, do not guess what to do, and do not start the local run. Instead run the line below **with the Bash tool** to send a toast, keeping the string exactly as written.

powershell.exe -NoProfile -File "D:\dont_move\git_save\Daily_Task\shared\notify.ps1" -Message "Event Scout run stalled / SCHEDULED_RUN.md not found / needs attention" -Title "Event Scout"

Then stop. This toast is the only notification channel.
