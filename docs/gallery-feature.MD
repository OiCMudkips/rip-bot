# Gallery feature

## Background information

* This is a Discord bot that, among other things, keeps track of the number of times that a user has died in game in a "deaths" table. There is a list of existing Discord global command definitions in the `commands` directory.

## Existing backend features

* DB has list of deaths, who reported it, who died, S3 url for full image of death.
* Full DB schema for deaths:
```
CREATE TABLE "deaths" (
	"server"	TEXT NOT NULL,
	"channel_id"	TEXT NOT NULL,
	"message_id"	TEXT NOT NULL,
	"dead_person"	TEXT NOT NULL,
	"caption"	TEXT NOT NULL,
	"attachment"	TEXT NOT NULL,
	"image_url"	TEXT NOT NULL,
	"timestamp"	INTEGER NOT NULL,
	"reporter"	TEXT NOT NULL
)
```

## Constraints

 * Use `python` requests library for all API requests

## Idea

* As a user, be able to type in a command `/generate-gallery` to generate a gallery in Discord.
    * Command takes in the following argument: Target user (Discord global command type 6 option)
* The command should return a placeholder message for Discord saying: "Generating gallery for @target_user_id..."
* Next, the command should perform the following chain of tasks using Celery:
    1. Get list of users to get information for
        * Get all of the deaths from DB for the target user (target user is `dead_person` in the table)
            * Filter death results to current guild
            * Generate Celery task to get display name and profile picture for the target user (`is_target_user = true`)
            * Generate Celery tasks for every unique reporter to get the display name (`is_target_user = false`)
        * Return as output the list of deaths
            * Fields to include: `caption`, `image_url`, `timestamp`, `reporter`
    2. User's guild display name and profile picture
        * Task input: `guild_id` (str), `user_id` (str), `is_target_user` (bool)
        * Retrieve user's current guild display name (fallback to global name, then fallback to username), and if `is_target_user = true`, their profile picture hash, using Discord API
            * Specifically, use `/guilds/{guild.id}/members/{user.id}` endpoint to get guild member object
            * Use `avatar`, `nick` fields to get profile pictures, guild display name if they are available
            * Otherwise, use `user.global_name`, `user.username` and `user.avatar` field of the guild member object to get fallbacks
        * If `is_target_user = true`
            * Using the image hash from the previous step:
            * Download the user's profile picture
                * Use Discord's image formatting URLs to ensure that downloaded image is a png and sized 256 x 256 pixels
                * As described in https://docs.discord.com/developers/reference#image-formatting
            * Upload it to S3 bucket defined by environment variable `S3_BUCKET_USER_PROFILE_PICTURES`
        * TODO: figure out auth
        * TODO: find Discord API which would enable this in a server's context
        * If retrieval of updated information fails and `is_target_user = true`, return an error to cancel the whole command
        * If retrieval of updated information fails and `is_target_user = false`, return placeholder info
            * "Unknown user" as the display name string
        * Return as task output: user id, display name (or fallback), profile picture in S3 (nullable)
    3. Generate an HTML gallery page that shows all the images
        * Include user's details at the top in a header (display name, profile picture) from output of step 2
        * Include generation date in the header
        * Show all the pictures for the user's deaths with 3 on each row
        * Each picture's source is in the output of step 1
        * Each picture should be width 22.5% of the window width
        * Each picture should be clickable to open a modal
        * The modal shows:
            * The picture at 50% width of the window
            * Death reporter's name (from output of step 2)
            * Time of reporting (from output of step 1)
            * Caption (from output of step 1)
            * Close button
        * Do not use React, use `mustache` templating library
        * Store the pre-rendered HTML page in a directory defined by an environment variable called `GALLERY_PAGES_OUTPUT_DIRECTORY`
        * HTML page should be a securely-random 10 character alphanumeric string followed by ".html"
        * An external app will serve this page at "https://rip-bot.com/gallery/{the random string}.html"
        * Return as task output this link
    4. Update the Discord command message
        * Using the output in step 3 for the gallery url, and the target user id from the Discord command arguments, update the Discord command message with the following content: "Your gallery for @target_user_id has been generated! [Link]($link)"
