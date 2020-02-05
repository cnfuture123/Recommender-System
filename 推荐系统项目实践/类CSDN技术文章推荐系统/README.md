# 类CSDN技术文章推荐系统

## 项目简介

  - 本项目是基于大数据离线和实时计算框架，利用用户行为构建用户与文章之间的画像关系，然后通过相应的推荐算法对文章进行智能推荐。

## 项目架构

## 数据集

  - 本项目数据集主要包括三类数据：用户信息，文章信息，用户行为数据。
  - 用户信息（共120个用户）：user_basic，user_profile。
    - user_basic字段：user_id, mobile, password, profile_photo, last_login, is_media, article_count, following_count, fans_count, like_count, read_count, introduction, certificate, is_verified。
    - user_profile字段：user_id, gender, birthday, real_name, create_time, update_time, register_media_time, id_number, id_card_front, id_card_back, id_card_handheld, area, company, career。
  - 文章信息（共1000篇文章）：news_article_basic, news_article_content, news_channel。
    - news_article_basic字段：article_id, user_id, channel_id, title, status, update_time。
    - news_article_content字段：article_id, content。
    - news_channel字段：channel_id, channel_name, create_time, update_time, sequence, is_visible, is_default。
  - 用户行为（共3000条用户行为数据）：user_action
    - JSON格式，包含字段：actionTime, readTime, channelId, action, userId, articleId, algorithmCombine。
      ```
      {"actionTime":"2019-03-12 10:34:52","readTime":"","channelId":3,"param":{"action": "exposure", "userId": "15", "articleId": "[32, 94, 51]", "algorithmCombine": "C2"}}
      ```
