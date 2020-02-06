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

## 系统模块

### 离线画像

  - 基于文章数据构建文章画像，主要包括频道，关键词，主题词等。
  - 基于用户信息和用户行为构建用户画像。
  
#### 离线文章画像

  - 构建离线文章画像流程：
    - 将文章标题，频道以及内容整合为文章数据，用来构建文章画像。
    - 通过TF-IDF和TextRank算法分别对文章数据进行分词，并选取权重大的作为关键词。
    - 将两种算法提取的关键词的交集作为该文章的主题词。
    - 将结果保存在Hive表中作为文章画像信息。
    - ![离线文章画像代码参考](./代码/文章画像)
  - 文章相似度计算：
    - 基于文章数据训练Word2Vec模型。
    - 利用训练好的模型将文章画像的关键词转化为词向量。
    - 计算平均词向量作为文章向量。
    - 采用Spark的BucketedRandomProjectionLSH算法基于文章向量进行文章相似度的计算。
    - 文章相似度结果保存在HBase表中。
    - ![文章相似度计算代码参考](./代码/文章相似度)
    
#### 离线用户画像

  - 构建离线用户画像流程：
    - 对收集到的用户日志(user_action)进行处理，并提取用户行为信息。
    - 将用户发生过行为的文章主题词提取出来，作为该用户的标签。
    - 根据不同的用户行为给相应的用户标签赋予权重：
      - read and read_time < 1000 : 1
      - read and read_time > 1000 : 2
      - collect : 2
      - share : 3
      - click : 1
    - 将用户标签和相应的权重保存在Hive表中。
    - ![离线用户画像代码参考](./代码/用户画像)
  
### 离线召回

  - 基于模型的召回：ALS算法
  - 基于内容的召回：文章相似度
  
#### 基于模型的召回

  - 基于模型的召回流程：
    - 通过Spark的ALS算法对用户的点击行为数据训练模型。
    - 用训练的模型对所有用户生成推荐列表。
    - 将生成的推荐列表存入HBase表中。
    - ![基于模型的召回代码参考](./代码/基于模型的召回)
  - 基于内容的召回流程：
    - 过滤出用户点击过的所有文章。
    - 计算得出与这些文章相似性大的文章，生成推荐列表。
    - 将推荐结果保存在HBase表中。
    - ![基于内容的召回代码参考](./代码/基于内容的召回)
    
### 离线排序

  - 基于LR的点击率预估
  - 离线CTR特征中心
  
#### 基于LR的点击率预估

  - 基于LR的点击率预估流程：
    - 综合用户画像，文章画像以及用户点击行为提取特征，预测用户是否点击文章为目标值。
    - 用Spark的LR模型对提取的训练样本进行训练。
    - 用训练好的模型预测用户对某篇文章的点击率。
    - ![基于LR的点击率预估代码参考](./代码/基于LR的点击率预估)
    
#### 离线CTR特征中心

  - 离线CTR特征中心流程：
    - 用户特征中心：基于用户画像构造用户特征，并保存在HBase表中。
    - 文章特征中心：基于文章画像构造文章特征，并保存在HBase表中。
    - ![离线CTR特征中心代码参考](./代码/离线CTR特征中心)
  
### 实时召回

  - 实时召回流程：
    - Flume --> Kafka --> Spark Streaming
    - Flume收集用户点击日志，并传入指定的Kafka Topic。
    - Spark Streaming读取Kafka Topic中的数据，并找到与点击文章相似的文章生成推荐列表。
    - 将推荐列表保存在HBase表中。
    - ![实时召回代码参考](./代码/实时召回)
    
  
    
    
  
  
  
  
    
    
  
