# Twitter Insight

Student project, one-month tweets analysis using Hadoop framework.
The project is cut into 5 different packages:
- `datacleaning`: clean the raw data

- `wordcount`: list of hashtags and their number of occurrences
- `topk`: list of the K top-used hashtags

- `hashtagbyuser`: list the users and their hashtags
- `triplet`: list of the triplets of hashtags and their users

## Requirements

- make
- maven

## Installation

Go to the project root.

`make build`: package the modules all at once.

If you prefer to package the modules one by one:
- `make datacleaning`
- `make wordcount`
- `make topk`
- `make hashtagbyuser`
- `make triplet`

## Usage

### Cleaner

`yarn jar Projet-Tweeter-1.0.jar ${tweet} clean_data`: produces 164 sequence files into the `clean_data` folder.  

### Hashtag analysis

`yarn jar Projet-Tweeter-TopHashtags-1.0.jar clean_data count_hashtags top_hashtags [K]`: produces 2 output folders `count_hashtags` and `top_hashtags`.

By default K=10.

`hdfs dfs -text count_hashtags/part-r-<>`: Visualize the results of the word count pattern.
`hdfs dfs -head top_hashtags/part-m-<>`: Visualize the results of the top k pattern.

### Data Mining

`yarn jar Projet-Tweeter-HashtagByUser-1.0.jar clean_data hashtag_by_user`: produces 1 output folder `count_hashtags`.

`hdfs dfs -text hashtag_by_user/part-r-<>`: Visualize the users (userId, userName) and their hashtags.

### Social Network

`yarn jar Projet-Tweeter-HashtagByUser-1.0.jar hashtag_by_user hashtag_triplets`: produces 1 output folder `count_hashtags`.

`hdfs dfs -text count_hashtags/part-r-<>`: Visualize the triplets of hashtags and their users.

// TODO: wordcount and topk on top of the triplets

## Authors and acknowledgment

Deborah Pereira & Sophie Stan.

Supervising teacher: David Auber.
