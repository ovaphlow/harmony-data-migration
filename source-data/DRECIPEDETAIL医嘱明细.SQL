/*
 Navicat Premium Data Transfer

 Source Server         : 10.4 SqlServer
 Source Server Type    : SQL Server
 Source Server Version : 10501600
 Source Host           : localhost:1433
 Source Catalog        : fourthWelfare
 Source Schema         : dbo

 Target Server Type    : SQL Server
 Target Server Version : 10501600
 File Encoding         : 65001

 Date: 20/11/2025 16:15:59
*/


-- ----------------------------
-- Table structure for DRecipeDetail
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[DRecipeDetail]') AND type IN ('U'))
	DROP TABLE [dbo].[DRecipeDetail]
GO

CREATE TABLE [dbo].[DRecipeDetail] (
  [DetailId] int  IDENTITY(1,1) NOT NULL,--id
  [IllProgressId] int  NULL,--关联病程录表illprogress的IllProgressId
  [LongId] int  NULL,--长期医嘱表drecipelong的longid
  [RecipeId] int  NULL,--关联drecipe表的RecipeId
  [DrugId] int  NULL,--关联药品信息表的drugid
  [ElderId] int  NULL,--老人id
  [illerName] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [company] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [RecipeDoctor] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--开方医生
  [RecipeDate] datetime  NULL,--开方日期
  [RecipeKind] varchar(20) COLLATE Chinese_PRC_CI_AS  NOT NULL,--医嘱类型（长期、临时）
  [RecipeAmount] numeric(18,2)  NULL,--处方数量
  [TradePrice] money  NULL,--零售价
  [CostPrice] money  NULL,--成本价（应该就是进货价）
  [UseRoute] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--给药途径
  [Usemethod] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--给药方法
  [TimesAmount] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,--次用量
  [Prompt] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,--提示
  [IsVerify] char(1) COLLATE Chinese_PRC_CI_AS DEFAULT ('0') NULL,--是否核对（1：是；0：否）
  [VerifyDate] datetime  NULL,--核对时间
  [VerifyMan] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--核对人
  [IsBill] char(1) COLLATE Chinese_PRC_CI_AS DEFAULT ('0') NULL,--是否付费？
  [BillDate] datetime  NULL,--付费时间
  [BillMan] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--付费人（都是空）
  [IsSend] char(1) COLLATE Chinese_PRC_CI_AS DEFAULT ('0') NULL,--是否发药
  [SendDate] datetime  NULL,--发药日期
  [SendMan] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--发药人
  [ReturnAmount] numeric(18,2) DEFAULT ((0)) NULL,--退药数量
  [RecipeInputAmount] numeric(18,3)  NULL,--估计没用
  [RecipeInputUnit] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [bedId] int  NULL,--关联床位表bed的bedid
  [groupNumber] varchar(2) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [printAmount] int  NULL,--打印次数（现在应该没用了）
  [isPrint] char(1) COLLATE Chinese_PRC_CI_AS  NULL,--是否打印
  [recipeTime] datetime  NULL,--估计没用
  [printTime] datetime  NULL,--打印时间
  [bednum] varchar(10) COLLATE Chinese_PRC_CI_AS  NULL,--床位号
  [printMan] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--打印人
  [TimesAmount2] numeric(18,4)  NULL,--次用量
  [TimesAmountMorning] numeric(18,4)  NULL,--次用量（早）
  [TimesAmountMiddle] numeric(18,4)  NULL,--次用量（中）
  [TimesAmountNight] numeric(18,4)  NULL,--次用量（晚）
  [TimesAmountEvening] numeric(18,4)  NULL,--次用量（夜）
  [timesAmountUnit] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--次用量单位
  [timesName] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [groupOrder] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,--估计没用
  [totalId] int  NULL,--未知？
  [currentRemainAmount] numeric(18,2)  NULL,--估计没用
  [fillAdviseId] int  NULL,--估计没用
  [isTempAdvise] char(1) COLLATE Chinese_PRC_CI_AS  NULL--估计没用
)
GO

ALTER TABLE [dbo].[DRecipeDetail] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of DRecipeDetail
-- ----------------------------
SET IDENTITY_INSERT [dbo].[DRecipeDetail] ON
GO