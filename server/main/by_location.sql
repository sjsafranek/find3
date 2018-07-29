{
    "locations": [
        {
            "devices": [
                {
                    "device": "wifi-88:d7:f6:a7:2a:48",
                    "vendor": "ASUSTek Computer Inc.",
                    "timestamp": "2018-03-10T11:29:33.063Z",
                    "probability": 0.89,
                    "randomized": false,
                    "num_scanners": 3,
                    "active_mins": 1295,
                    "first_seen": "2018-03-09T06:58:21.327Z"
                },
                {
                    "device": "wifi-40:4e:36:89:63:a5",
                    "vendor": "HTC Corporation",
                    "timestamp": "2018-03-10T11:25:34.469Z",
                    "probability": 0.83,
                    "randomized": false,
                    "num_scanners": 3,
                    "active_mins": 815,
                    "first_seen": "2018-03-09T07:16:49.934Z"
                }
            ],
            "location": "desk",
            "total": 2
        },
        {
            "devices": [
                {
                    "device": "wifi-20:df:b9:49:1c:61",
                    "vendor": "Google, Inc.",
                    "timestamp": "2018-03-10T11:29:33.043Z",
                    "probability": 0.88,
                    "randomized": false,
                    "num_scanners": 3,
                    "active_mins": 1123,
                    "first_seen": "2018-03-09T06:59:34.364Z"
                }
            ],
            "location": "kitchen",
            "total": 1
        }
    ],
    "message": "got locations",
    "success": true
}




return `
    '{'||
        '"id": "' ||  letter_firstid ||'",'||
        '"timestamp": ' || strftime('%s',time) ||','||
        '"recipients": ' ||  letter_to ||','||
        '"owner_id": "' ||  sender ||'",'||
        '"owner_name": "' || IFNULL((SELECT letter_content FROM letters WHERE opened == 1 AND letter_purpose == 'action-assign/name' AND sender == ltr.sender ORDER BY time DESC LIMIT 1), 'null') ||'",'||
        '"content": "' ||  replace(letter_content, '"',  '''') ||'",'||
        '"reply_to": "' ||  letter_replyto ||'",'||
        '"purpose":"' ||  letter_purpose ||'",'||
        '"likes": '|| (SELECT COUNT(*) FROM letters WHERE opened == 1 AND letter_purpose == 'action-like' AND letter_content=ltr.letter_firstid) ||','||
        '"num_comments": '|| ( SELECT count(*) FROM letters WHERE opened == 1 AND letter_purpose = 'share-text' AND letter_replyto = ltr.letter_firstid ) ||','||
        '"hashtags": [' ||
            (SELECT IFNULL(GROUP_CONCAT(tag), '') FROM (
                SELECT '"'||tag||'"' AS tag FROM tags WHERE tags.e_id=ltr.id
            ))
        || ']'
    ||'}'
`








SELECT
    *
FROM
    keystore
LEFT JOIN
    device;


SELECT * FROM location_predictions
    INNER JOIN sensors
    ON sensors.timestamp = location_predictions.timestamp;
