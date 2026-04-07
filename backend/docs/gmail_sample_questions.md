# Gmail Connector — Sample Questions

Natural language questions you can ask DataQL against your Gmail inbox.
Each question is translated into SQL by the AI query planner.

---

## Inbox Overview

| Question | Generated SQL |
|---|---|
| How many unread emails do I have? | `SELECT COUNT(*) as unread_count FROM gmail_messages WHERE is_unread = 1` |
| How many emails do I have total? | `SELECT COUNT(*) as total FROM gmail_messages` |
| Show me my email labels and unread counts | `SELECT name, messages_total, messages_unread FROM gmail_labels ORDER BY messages_unread DESC` |

## Senders & Contacts

| Question | Generated SQL |
|---|---|
| Who sends me the most emails? | `SELECT from_email, from_name, COUNT(*) as email_count FROM gmail_messages GROUP BY from_email, from_name ORDER BY email_count DESC LIMIT 10` |
| How many unique people have emailed me? | `SELECT COUNT(DISTINCT from_email) as unique_senders FROM gmail_messages` |
| Show me emails from a specific sender | `SELECT subject, date, snippet FROM gmail_messages WHERE from_email LIKE '%@example.com%' ORDER BY date_unix DESC` |

## Time-Based

| Question | Generated SQL |
|---|---|
| Show me emails I received today | `SELECT from_name, subject, date FROM gmail_messages WHERE date LIKE '2026-04-06%' ORDER BY date_unix DESC` |
| How many emails did I get this week? | `SELECT COUNT(*) as count FROM gmail_messages WHERE date >= date('now', '-7 days')` |
| Show me my most recent 10 emails | `SELECT from_name, subject, date FROM gmail_messages ORDER BY date_unix DESC LIMIT 10` |

## Attachments & Content

| Question | Generated SQL |
|---|---|
| Which emails have attachments? | `SELECT from_name, subject, date FROM gmail_messages WHERE has_attachments = 1 ORDER BY date_unix DESC` |
| How many emails have attachments? | `SELECT COUNT(*) as count FROM gmail_messages WHERE has_attachments = 1` |
| Search emails about a topic | `SELECT from_name, subject, snippet, date FROM gmail_messages WHERE subject LIKE '%invoice%' OR snippet LIKE '%invoice%' ORDER BY date_unix DESC` |

## Advanced

| Question | Generated SQL |
|---|---|
| Who sends me the most unread emails? | `SELECT from_name, from_email, COUNT(*) as unread FROM gmail_messages WHERE is_unread = 1 GROUP BY from_email, from_name ORDER BY unread DESC LIMIT 10` |
| Show me email volume by day | `SELECT substr(date, 1, 10) as day, COUNT(*) as count FROM gmail_messages GROUP BY day ORDER BY day DESC LIMIT 30` |
| Which threads have the most messages? | `SELECT thread_id, COUNT(*) as msg_count, MIN(subject) as subject FROM gmail_messages GROUP BY thread_id ORDER BY msg_count DESC LIMIT 10` |

---

> **Note:** Gmail connector fetches up to 200 messages (configurable via `GMAIL_MAX_MESSAGES` in `.env`).
> First query takes ~15–30s to sync; subsequent queries use a 2-minute cache.
