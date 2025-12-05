I've got an idea I want to talk through with you. I'd like you to help me turn it into a fully formed design and spec (and eventually an implementation plan) Check out the current state of the project in our working directory to understand where we're starting off and then check the idea details below. Once done, ask me questions, one at a time, to help refine the idea. Ideally, the questions would be multiple choice, but open-ended questions are OK, too. Don't forget: only one question per message. Once you believe you understand what we're doing, stop and describe the design to me, in sections of maybe 200-300 words at a time, asking after each section whether it looks right so far. Keep in mind that whatever we fix here will affect the upstream repo /Users/taariqlewis/Projects/Seren_Projects/seren-replicator so all changes must be refactored upstream.

Here's the idea

1.would it be better UI/UX to remove the need for users to use the connecton string in the CLI for the databse the replicator/
2. Instead have users use their SerenDB API key Alone
3. The API Key allows users to lists all projects and all databases so the user just needs to select their target database to replicate against. 
4. And then the connection_string can be read from the project by the database replicator instead of the user entering it. Easier UI/UX.
5. This will also fix the `sync` issue where there's no direct API to look up a project by endpoint hostname.
6. The API is setup for users to adjust settings on project.
7. When init is run, the user selects their target project and then database. No more need for connection string.
8. When sync is run, the system selects their already targeted project and database so they are syncing to the same database. We want to avoid users having to be confused by different branches so just make sure they are syncing to same database.
9. Existing APIs:
  
  GET  /api/projects                              → List user's projects
  GET  /api/projects/{project_id}/replication     → Get replication settings
  PATCH /api/projects/{project_id}/replication    → Enable logical replication (set enabled: true)
  GET  /api/projects/{project_id}/branches        → List branches
  GET  /api/projects/{project_id}/branches/{branch_id}/endpoints  → List endpoints
  GET  /api/projects/{project_id}/branches/{branch_id}/databases  → List databases

 10. Proposed UPDATED Replicator flow:

USER enters their API KEY  
GET /api/projects → Show project picker
GET /api/projects/{id}/replication → Check if logical replication is enabled
If not enabled: PATCH /api/projects/{id}/replication with {"enabled": true}
USE the DEFAUTL branch from the project
GET /api/projects/{id}/branches/{bid}/databases → Show database picker
Start replication using the selected database's connection string
Store replication target for sync run so that sync is to the same replication target.

