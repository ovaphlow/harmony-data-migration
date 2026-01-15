/*
 Navicat Premium Data Transfer

 Source Server         : sqlserver
 Source Server Type    : SQL Server
 Source Server Version : 10501600
 Source Host           : .:1433
 Source Catalog        : fourthWelfare
 Source Schema         : dbo

 Target Server Type    : SQL Server
 Target Server Version : 10501600
 File Encoding         : 65001

 Date: 07/01/2026 17:40:24
*/


-- ----------------------------
-- Table structure for MRTotal
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[MRTotal]') AND type IN ('U'))
	DROP TABLE [dbo].[MRTotal]
GO

CREATE TABLE [dbo].[MRTotal] (
  [RecordId] int  IDENTITY(1,1) NOT NULL,
  [ElderId] int  NULL,
  [Talker] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,
  [BealiveCondition] varchar(10) COLLATE Chinese_PRC_CI_AS  NULL,
  [Doctor] varchar(10) COLLATE Chinese_PRC_CI_AS  NULL,
  [RecordDate] datetime  NULL,
  [IllName] varchar(300) COLLATE Chinese_PRC_CI_AS  NULL,
  [IllItemName] varchar(2000) COLLATE Chinese_PRC_CI_AS  NULL,
  [Talker2] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,
  [helpDoctor] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,
  [summary] varchar(2000) COLLATE Chinese_PRC_CI_AS  NULL
)
GO

ALTER TABLE [dbo].[MRTotal] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of MRTotal
-- ----------------------------
SET IDENTITY_INSERT [dbo].[MRTotal] ON
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'55', N'4', N'患者本人', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'54', N'9', N'患者本人', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'168', N'17', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'161', N'41', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'169', N'46', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'56', N'51', N'患者家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'173', N'52', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'150', N'61', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'62', N'65', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'73', N'66', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'136', N'74', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'69', N'91', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'174', N'93', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'133', N'96', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'78', N'98', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'176', N'99', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'67', N'106', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'156', N'115', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'164', N'120', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'131', N'144', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'144', N'163', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'52', N'167', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'179', N'169', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'72', N'178', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'68', N'181', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'152', N'191', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'146', N'193', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'172', N'201', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'59', N'215', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'75', N'218', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'167', N'224', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'26', N'239', N'其他', N'', N'陈意东', N'2015-06-19 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'65', N'243', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'181', N'248', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'143', N'260', N'患者本人及家属', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'58', N'262', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'160', N'266', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'154', N'269', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'137', N'270', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'142', N'274', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'83', N'275', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'97', N'279', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'92', N'288', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'47', N'299', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'49', N'306', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'148', N'326', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'129', N'340', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'178', N'341', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'57', N'342', N'患者本人', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'170', N'343', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'157', N'362', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'86', N'363', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'118', N'371', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'151', N'376', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'165', N'401', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'94', N'406', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'158', N'409', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'166', N'411', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'183', N'416', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'134', N'422', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'120', N'426', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'163', N'431', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'50', N'432', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'89', N'440', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'71', N'442', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'51', N'443', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'91', N'448', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'74', N'457', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'140', N'460', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'175', N'889', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'116', N'899', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'109', N'898', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'4', N'896', N'患者家属', N'可靠', N'刘丕', N'2014-02-25 00:00:00.000', NULL, NULL, N'2222', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'117', N'897', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'7', N'900', N'其他', N'可靠', N'陈意东', N'2014-03-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'111', N'901', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'6', N'902', N'其他', N'基本可靠', N'陈意东', N'2014-03-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'105', N'903', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'114', N'904', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'122', N'905', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'104', N'906', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'128', N'907', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'15', N'908', N'其他', N'可靠', N'刘丕', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'17', N'909', N'其他', N'可靠', N'刘丕', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'18', N'910', N'其他', N'可靠', N'刘丕', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'19', N'924', N'其他', N'基本可靠', N'陈意东', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'20', N'911', N'其他', N'基本可靠', N'陈意东', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'16', N'912', N'其他', N'基本可靠', N'陈意东', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'13', N'913', N'其他', N'基本可靠', N'陈意东', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'11', N'914', N'其他', N'可靠', N'王屹', N'2014-04-14 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'23', N'915', N'其他', N'可靠', N'王屹', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'22', N'916', N'其他', N'可靠', N'王屹', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'21', N'917', N'其他', N'可靠', N'王屹', N'2014-04-14 00:00:00.000', N'', N'', NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'9', N'918', N'其他', N'基本可靠', N'王成', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'8', N'919', N'其他', N'基本可靠', N'王成', N'2014-04-14 00:00:00.000', NULL, NULL, N'市二福院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'10', N'920', N'其他', N'基本可靠', N'王成', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'12', N'921', N'其他', N'基本可靠', N'杨雪林', N'2014-04-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'36', N'925', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'35', N'926', N'其他', N'可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'33', N'927', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'32', N'928', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'37', N'933', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'34', N'935', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'31', N'937', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'43', N'929', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'42', N'930', N'其他', N'基本可靠', N'王屹', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'44', N'931', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'39', N'932', N'其他', N'可靠', N'刘丕', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'40', N'934', N'其他', N'可靠', N'刘丕', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'41', N'936', N'其他', N'可靠', N'刘丕', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'46', N'938', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'45', N'939', N'其他', N'基本可靠', N'陈意东', N'2015-07-01 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'185', N'940', N'其他', N'基本可靠', N'高锡英', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'188', N'941', N'其他', N'可靠', N'徐英', N'2016-04-06 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'189', N'946', N'其他', N'基本可靠', N'高锡英', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'186', N'942', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'205', N'943', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'王屹', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'235', N'960', N'其他', N'可靠', N'袁纯兰', N'2017-01-12 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'王屹', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'195', N'944', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'190', N'945', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'200', N'947', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'199', N'948', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'201', N'949', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'202', N'950', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'192', N'951', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'203', N'952', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'198', N'953', N'其他', N'基本可靠', N'周婷', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'187', N'954', N'其他', N'基本可靠', N'王屹', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 2.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 3.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'196', N'955', N'其他', N'可靠', N'袁纯兰', N'2016-04-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'194', N'957', N'', N'可靠', N'王屹', N'2016-04-06 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'204', N'958', N'其他', N'可靠', N'周婷', N'2016-04-06 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'193', N'956', N'其他', N'可靠', N'刘丕', N'2016-04-06 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'上海市第二社会福利院', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'225', N'959', N'其他', N'可靠', N'周婷', N'2017-01-12 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'333', N'1067', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'334', N'1054', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'336', N'1059', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'231', N'961', N'其他', N'可靠', N'胡新志', N'2017-01-12 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'232', N'963', N'其他', N'可靠', N'胡新志', N'2017-01-12 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'234', N'965', N'其他', N'可靠', N'胡新志', N'2017-01-12 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'237', N'968', N'其他', N'可靠', N'袁纯兰', N'2017-01-12 00:00:00.000', N'精神发育迟滞（极重度）', N'', N'上海市第二社会福利院', N'王屹', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'229', N'969', N'其他', N'可靠', N'袁纯兰', N'2017-01-12 00:00:00.000', N'精神发育迟滞（极重度）', N'', N'上海市第二社会福利院', N'王屹', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'236', N'967', N'其他', N'可靠', N'袁纯兰', N'2017-01-12 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'王屹', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'215', N'966', N'其他', N'可靠', N'胡新志', N'2017-01-12 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'230', N'964', N'其他', N'可靠', N'周婷', N'2017-01-12 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'233', N'962', N'其他', N'可靠', N'周婷', N'2017-01-12 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'244', N'970', N'患者家属', N'基本可靠', N'周婷', N'2017-07-25 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'243', N'971', N'患者家属', N'可靠', N'袁纯兰', N'2017-07-25 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'242', N'972', N'患者家属', N'基本可靠', N'胡新志', N'2017-07-25 00:00:00.000', N'骨折后遗症', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'245', N'973', N'患者家属', N'基本可靠', N'胡新志', N'2017-08-04 00:00:00.000', N'心肌梗死', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'246', N'974', N'患者家属', N'可靠', N'袁纯兰', N'2017-08-04 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'247', N'975', N'患者家属', N'基本可靠', N'胡新志', N'2017-08-07 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'248', N'976', N'患者家属', N'可靠', N'袁纯兰', N'2017-08-09 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'250', N'977', N'患者家属', N'基本可靠', N'胡新志', N'2017-08-11 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'249', N'978', N'患者家属', N'可靠', N'袁纯兰', N'2017-08-11 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'251', N'979', N'患者家属', N'基本可靠', N'胡新志', N'2017-08-15 00:00:00.000', N'精神发育迟滞（极重度）', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'253', N'980', N'患者家属', N'基本可靠', N'周婷', N'2017-08-16 00:00:00.000', N'帕金森氏症', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'252', N'981', N'患者家属', N'可靠', N'袁纯兰', N'2017-08-16 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'254', N'982', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-08-18 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'255', N'983', N'患者家属', N'基本可靠', N'胡新志', N'2017-08-22 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'256', N'984', N'患者家属', N'基本可靠', N'周婷', N'2017-08-31 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'257', N'985', N'患者家属', N'可靠', N'袁纯兰', N'2017-08-31 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'258', N'986', N'患者本人', N'基本可靠', N'胡新志', N'2017-09-11 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'259', N'987', N'患者家属', N'基本可靠', N'胡新志', N'2017-10-19 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'260', N'988', N'其他', N'基本可靠', N'胡新志', N'2017-10-25 00:00:00.000', N'精神发育迟滞（极重度）', N'', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'264', N'989', N'其他', N'基本可靠', N'周婷', N'2017-10-26 00:00:00.000', N'抑郁倾向', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'270', N'990', N'其他', N'基本可靠', N'周婷', N'2017-10-26 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'261', N'991', N'其他', N'基本可靠', N'胡新志', N'2017-10-26 00:00:00.000', N'小儿麻痹症后遗症', N'', N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'262', N'992', N'其他', N'基本可靠', N'周婷', N'2017-10-26 00:00:00.000', N'精神发育迟滞（极重度）', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'263', N'993', N'其他', N'可靠', N'袁纯兰', N'2017-10-25 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'268', N'994', N'其他', N'可靠', N'袁纯兰', N'2017-10-25 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'280', N'1003', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-11-28 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'281', N'997', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-11-28 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'274', N'999', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-11-28 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'271', N'998', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-11-28 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'277', N'1004', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-11-28 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'286', N'1005', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-11-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'275', N'1006', N'患者本人及家属', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'心肌梗死', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'273', N'1008', N'患者家属', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'272', N'1009', N'患者本人及家属', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'心肌梗死', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'276', N'1010', N'患者本人', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'279', N'1007', N'患者本人及家属', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'278', N'1000', N'其他', N'基本可靠', N'胡新志', N'2017-11-28 00:00:00.000', N'脑血管意外', N'', N'仓桥敬老院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'282', N'1002', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-11-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'284', N'995', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-11-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'285', N'1001', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-11-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'283', N'996', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-11-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'292', N'1011', N'患者本人及家属', N'基本可靠', N'胡新志', N'2017-12-06 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'290', N'1012', N'其他', N'基本可靠', N'胡新志', N'2017-12-06 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'仓桥敬老院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'300', N'1013', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'294', N'1014', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'299', N'1022', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'289', N'1015', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'293', N'1016', N'患者本人及家属', N'', N'周婷', N'2017-12-06 00:00:00.000', N'', N'', NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'288', N'1020', N'患者本人及家属', N'', N'周婷', N'2017-12-06 00:00:00.000', N'', N'', NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'297', N'1019', N'患者本人及家属', N'可靠', N'袁纯兰', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'296', N'1023', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-12-07 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'295', N'1018', N'患者本人及家属', N'基本可靠', N'周婷', N'2017-12-07 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'291', N'1017', N'患者本人及家属', N'基本可靠', N'胡新志', N'2017-12-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'298', N'1021', N'患者家属', N'基本可靠', N'胡新志', N'2017-12-10 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'301', N'1024', N'患者家属', N'基本可靠', N'胡新志', N'2017-12-23 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'304', N'1025', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-03-29 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'303', N'1026', N'患者本人及家属', N'基本可靠', N'周婷', N'2018-03-29 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'302', N'1027', N'患者本人及家属', N'基本可靠', N'胡新志', N'2018-03-29 00:00:00.000', N'心肌梗死', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'306', N'1028', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-03-30 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'307', N'1029', N'患者家属', N'基本可靠', N'胡新志', N'2018-03-30 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'308', N'1030', N'患者本人及家属', N'基本可靠', N'周婷', N'2018-04-10 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'309', N'1031', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-05-29 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'310', N'1032', N'患者本人及家属', N'基本可靠', N'胡新志', N'2018-06-14 00:00:00.000', N'心律失常', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'311', N'1033', N'其他', N'基本可靠', N'周婷', N'2018-06-14 00:00:00.000', N'', N'', N'第二福利院工作人员', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'312', N'1034', N'其他', N'基本可靠', N'胡新志', N'2018-06-14 00:00:00.000', N'癫痫', N'', N'上海市第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'313', N'1035', N'其他', N'可靠', N'袁纯兰', N'2018-06-14 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'314', N'1036', N'患者家属', N'基本可靠', N'胡新志', N'2018-06-22 00:00:00.000', N'老年性痴呆', N'1、TIA：该疾病为局部脑供血不足所致脑功能短暂丧失的发作，临床特点为局部脑功能的突然缺失，多数可在24小时内恢复，头颅CT检查无阳性发现。| 1.抑郁症：发病日期比较明显，常常伴随其他精神症状，认识功能衰退并不全面，尚能保持较好的注意力、记忆力和计算力。对检查有明显的不配合倾向，服用抑郁药症状可改善.| 2.皮质下动脉硬化性脑病：影像学表现为大脑前部或脑室周围皮质下白质缺血性损害，常首先表现为步态异常，特点是共济失调，但站立不稳，起步困难，行走缓慢，数周或数月内进展逐渐出现痴呆症状。', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'315', N'1037', N'患者本人及家属', N'基本可靠', N'周婷', N'2018-06-22 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'317', N'1038', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-06-26 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'316', N'1039', N'患者本人及家属', N'基本可靠', N'胡新志', N'2018-06-26 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。| 2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'318', N'1040', N'患者本人及家属', N'基本可靠', N'胡新志', N'2018-06-28 00:00:00.000', N'帕金森氏症', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'319', N'1041', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-06-28 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'320', N'1042', N'患者本人及家属', N'基本可靠', N'周婷', N'2018-07-04 00:00:00.000', N'', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'321', N'1043', N'患者家属', N'基本可靠', N'胡新志', N'2018-07-04 00:00:00.000', N'脑血管意外', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'322', N'1044', N'患者本人及家属', N'可靠', N'袁纯兰', N'2018-07-20 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'153', N'506', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'61', N'556', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'180', N'600', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'76', N'610', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'155', N'613', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'159', N'627', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'182', N'639', N'患者本人', N'', N'郁美华', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'48', N'641', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'141', N'644', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'66', N'647', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'64', N'649', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'102', N'654', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'85', N'658', N'患者本人及家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'101', N'660', N'患者本人及家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'103', N'665', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'119', N'666', N'患者本人及家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'135', N'669', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'100', N'671', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'28', N'673', N'', N'', N'刘丕', N'2015-06-24 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'99', N'685', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'147', N'689', N'患者本人及家属', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'121', N'691', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'82', N'692', N'患者本人及家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'63', N'702', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'106', N'703', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'80', N'714', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'81', N'716', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'149', N'728', N'患者家属', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'53', N'736', N'患者本人及家属', N'', N'陈意东', N'2015-07-25 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'84', N'747', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'79', N'751', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'70', N'775', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'130', N'779', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'110', N'788', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'93', N'798', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'132', N'818', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'77', N'819', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'88', N'846', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'95', N'857', N'患者本人', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'98', N'872', N'患者家属', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'96', N'876', N'其他', N'', N'陈意东', N'2015-07-30 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'145', N'881', N'患者本人', N'', N'陈意东', N'2015-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'324', N'1047', N'患者家属', N'基本可信', N'胡新志', N'2018-09-20 00:00:00.000', N'高血压病', N'1.肾实质病变：也可以表现为血压升高，但多伴有血尿，蛋白尿，有不同程度的内生肌酐清除率下降，血尿素氮和肌酐增高等肾功能损害，本患者临床症状与之不符，可根据肝，肾功能等进一步排除。|2.库欣综合征：多数有高血压，是由于各种原因引起的糖皮质醇分泌过度伴不同程度的盐皮质醇和雄激素分泌增加引起，有向中性肥胖，满月脸，水牛背，肌肉萎缩，骨质疏松糖代谢紊乱等表现，本患者临床症状与之不符', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'325', N'1046', N'患者本人及家属', N'可信', N'袁纯兰', N'2018-09-19 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'326', N'1048', N'患者本人及家属', N'可信', N'袁纯兰', N'2018-09-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'327', N'1049', N'患者本人及家属', N'可信', N'袁纯兰', N'2018-10-19 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'328', N'1050', N'患者本人及家属', N'基本可信', N'胡新志', N'2018-11-03 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'332', N'1052', N'患者本人及家属', N'可信', N'袁纯兰', N'2018-11-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'337', N'1057', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'338', N'1073', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'339', N'1056', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'341', N'1068', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'342', N'1058', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'343', N'1070', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'344', N'1060', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'347', N'1066', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'348', N'1062', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'350', N'1082', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'323', N'1045', N'患者本人及家属', N'基本可信', N'胡新志', N'2018-09-20 00:00:00.000', N'糖耐量异常', NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'329', N'1051', N'患者本人及家属', N'可信', N'袁纯兰', N'2018-11-09 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'330', N'247', N'患者本人', N'可信', N'周婷', N'2018-11-19 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'354', N'1080', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'356', N'1074', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'357', N'1078', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'358', N'1076', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'360', N'1081', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'364', N'1086', N'患者家属', N'', N'张丁', N'2019-04-01 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'367', N'1089', N'患者本人及家属', N'基本可靠', N'张丁', N'2019-06-12 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'368', N'1090', N'患者家属', N'基本可靠', N'胡新志', N'2019-06-13 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'369', N'1091', N'患者家属', N'基本可靠', N'胡新志', N'2019-06-17 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'370', N'1092', N'患者本人及家属', N'基本可靠', N'胡新志', N'2019-07-25 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'374', N'1096', N'患者本人及家属', N'基本可靠', N'张丁', N'2019-09-27 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'376', N'1098', N'患者本人及家属', N'可靠', N'袁纯兰', N'2019-12-05 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'377', N'1099', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-01-09 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'378', N'1101', N'其他', N'可信', N'周婷', N'2020-05-11 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'380', N'1102', N'患者家属', N'可信', N'袁纯兰', N'2020-06-22 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'381', N'1103', N'患者本人及家属', N'可靠', N'袁纯兰', N'2020-07-22 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'382', N'1104', N'患者本人及家属', N'基本可靠', N'胡新志', N'2020-07-23 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'384', N'1106', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-07-29 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'390', N'1112', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-10-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'391', N'1113', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-10-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'392', N'1114', N'患者本人及家属', N'基本可信', N'胡新志', N'2021-01-09 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'393', N'1115', N'患者本人及家属', N'基本可靠', N'胡新志', N'2021-01-18 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'394', N'1116', N'患者本人及家属', N'基本可靠', N'胡新志', N'2021-01-20 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'396', N'1118', N'患者本人及家属', N'基本可信', N'周婷', N'2021-04-16 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'402', N'1125', N'患者本人及家属', N'基本可信', N'周婷', N'2021-04-29 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'405', N'1128', N'患者家属', N'基本可靠', N'胡新志', N'2021-09-27 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'408', N'1131', N'患者家属', N'基本可靠', N'胡新志', N'2021-10-13 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'413', N'1136', N'患者家属', N'基本可靠', N'胡新志', N'2021-11-10 00:00:00.000', NULL, NULL, N'', N'', N'储士芳，女，72岁，左侧肢体活动不利10月余。患者原有高血压病史十余年，长期口服氨氯地平片，血压控制不稳定，维持在110-160/80-110之间。于2021年1月18日突发言语不清肢体乏力，神志嗜睡，精神萎，左侧肢体肌力0级，即到上海市第一人民医院住院治疗，诊断“1.右侧基底节出血破入脑室2.高血压”，予以右侧开颅血肿消除，术后积极抗炎补液对症支持治疗，病情好转，遗留左侧肢体功能障碍后出院。2021年9月17日入住第五康复医院予以康复治疗。现因生活不能自理，左侧肢体活动障碍，于20211110自愿入住我院，患者一般情况尚可，轮椅推入病房，智能减退，言语不清，查体欠合作，饮食可，吞咽功能差，睡眠欠佳，大小便正常。否认肝炎、结核等传染病史。体检：体温 36.6℃，脉搏88次/分，呼吸18次/分，血压136/78mmHg。两肺呼吸音粗，心率88次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，左侧肢体偏瘫,左上肢肌力为1级,左下肢肌力为2级,右上肢肌力为4级,右下肢肌力为3级。生化:葡萄糖：6.68mmol/L（2021-11-09松江中心医院）。糖化血红蛋白：4.9%，低密度脂蛋白：3.21mmol/L（2021-11-09松江中心医院），甘油三酯：2.44mmol/L，高密度脂蛋白：1.04mmol/L，总胆固醇：5.06mmol/L。Ca：2.45mmol/L，Cl：107.03mmol/L，K：3.67mmol/L，Na：147.14mmol/L，P：1.41mmol/L（2021-11-09松江中心医院）。A/G：1.91，白蛋白：47.03g/L，谷丙转氨酶：31.74u/L，间接胆红素：7.04umol/L，碱性磷酸酶（ALP）：105.57u/L（2021-11-09松江中心医院），总胆红素：10.56umol/L，总蛋白：71.60g/L。球蛋白：24.57g/L。直接胆红素：3.52umol/L。肌酐：71.92umol/L，尿素氮：3.21mmol/L，尿酸：327.22umol/L（2021-11-09松江中心医院），白细胞：126u/L，管型：0u/L，尿胆原：阴性，酸碱度：7.0，酮体：-，上皮细胞：1u/L（2021-11-09松江中心医院），亚硝酸盐：+，隐血：阴性。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'414', N'1138', N'患者本人及家属', N'基本可靠', N'庄秋丽', N'2021-11-12 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'416', N'1140', N'患者本人及家属', N'可信', N'袁纯兰', N'2021-12-01 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'417', N'1139', N'患者家属', N'基本可靠', N'胡新志', N'2021-12-01 00:00:00.000', NULL, NULL, N'', N'', N'陈凤声，男，83岁，反复头晕头痛近二十年。患者二十年前无诱因反复出现头晕头痛不适，无恶心、呕吐，无视物旋转，无黑朦，无胸闷心悸不适，在松江中心医院就诊，诊断“高血压病”，长期口服氨氯地平片等药物，血压维持在110-170/70-100mmHg之间。十余年前无诱因突发反应迟钝、步态不稳，无意识不清，无恶心，无抽搐，到松江中心医院就诊，诊断：“脑血管病、脑梗死、高血压”予以住院治疗，病情好转出院，未遗留肢体功能障碍。两年前“因胸闷、心悸加重半天伴头晕”，到松江中心医院住院治疗，诊断：“冠心病、心功能不全III级、高血压3级 很高危、脑梗死后遗症、脂肪肝、胆囊炎、胆囊结石、肾功能不全、颈椎退行性病变、肺结节”病情好转出院，遗留双下肢行走困难，反应迟钝，智能明显减退，个人生活不能自理，于20211201自愿入住我院。患者一般情况尚可，轮椅推入病房，对答不切题，智能明显减退，查体合作，饮食可、睡眠尚可，大小便正常。否认肝炎、结核等传染病史，否认糖尿病史。有冠心病，胆囊炎、胆囊结石史。体检：体温 36℃，脉搏85次/分，呼吸20次/分，血压142/78mmHg。两肺呼吸音粗，85次/分，心律齐，无腹壁紧张，无压痛，四肢无水肿，左上肢肌力为4级,左下肢肌力为3级,右上肢肌力为4级,右下肢肌力为3级。生化:葡萄糖：11.7mmol/L（2021-11-30松江中心医院）。糖化血红蛋白：8.9%（2021-11-30松江中心医院），低密度脂蛋白：2.76mmol/L（2021-11-30松江中心医院），甘油三酯：2.06mmol/L，高密度脂蛋白：1.06mmol/L，总胆固醇：4.64mmol/L。Ca：2.40mmol/L，Cl：99.64mmol/L，K：4.36mmol/L，Na：137.58mmol/L，P：1.40mmol/L（2021-11-30松江中心医院）。A/G：1.00，白蛋白：39.62g/L，谷丙转氨酶：10.79u/L，间接胆红素：6.12umol/L，碱性磷酸酶（ALP）：130.00u/L（2021-11-30松江中心医院），总胆红素：9.46umol/L，总蛋白：79.13g/L。球蛋白：39.51g/L。直接胆红素：3.34umol/L。肌酐：119.97umol/L，尿素氮：5.59mmol/L，尿酸：297.38umol/L（2021-11-30松江中心医院），')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'418', N'1141', N'患者本人及家属', N'基本可靠', N'胡新志', N'2021-12-07 00:00:00.000', NULL, NULL, N'', N'', N'佘家樑，男，67岁，左侧肢体活动不利六年。患者平素体健，无高血压史。于2016年12月10日夜间小便时突发意识丧失、瘫倒在地，无恶心呕吐，无口吐白沫，即到松江中心医院就诊，诊断为：“脑梗死”住院治疗（具体治疗不详），病情好转出院，遗留口齿不清，左侧肢体活动不利、功能障碍，长期康复治疗。2020年12月4日不慎跌倒致左股骨颈骨折，在松江中心医院予以手术治疗，术后行走困难。于2021年4月在松江中心医院行左股骨头置换术。目前因个人生活不能自理，于20211207自愿入住我院。患者一般情况尚可，轮椅推入病房，反应迟钝，智能减退，口齿不清，对答尚切题，查体合作，饮食可、睡眠尚可，大便正常，长期留置导尿。否认肝炎、结核等传染病史，否认高血压、糖尿病史。体检：体温 36.4℃，脉搏80次/分，呼吸20次/分，血压110/75mmHg。两肺呼吸音清，80次/分，心律齐，四肢无水肿，左上肢肌力为2级,左下肢肌力为3级,右上肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：6.85mmol/L（2021-11-9松江中心医院）。C反应蛋白：0.76MG/L（2021-12-3松江中心医院），糖化血红蛋白：6.10%（2021-12-3松江中心医院），低密度脂蛋白：1.77mmol/L（2021-12-3松江中心医院），甘油三酯：0.63mmol/L，高密度脂蛋白：1.19mmol/L，总胆固醇：3.13mmol/L。Ca：2.32mmol/L，Cl：101.36mmol/L，K：4.20mmol/L，Na：138.19mmol/L，P：0.96mmol/L（2021-12-3松江中心医院）。A/G：1.18，白蛋白：38.80g/L，谷丙转氨酶：9.20u/L，碱性磷酸酶（ALP）：97.00u/L（2021-11-30松江中心医院），总胆红素：8.60umol/L，总蛋白：71.80g/L。直接胆红素：4.00umol/L。肌酐：59umol/L，尿素氮：4.16mmol/L，尿酸：222umol/L（2021-11-9松江中心医院），白细胞：695/ulu/L（2021-11-30松江中心医院），尿胆原：+，亚硝酸盐：++，隐血：++。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'419', N'1142', N'患者本人及家属', N'可信', N'庄秋丽', N'2021-12-21 00:00:00.000', NULL, NULL, NULL, NULL, N'尤福珍，女，90岁，患者左下肢行走不利3年余。患者2018年5月5日不慎跌倒致左髋左大腿外伤，于松江中心医院急诊作摄片检查为：左股骨骨折，当时据诉因为年纪大未做手术处理。目前下肢行走不利，平时拄拐缓慢尚能行走。
   患者原有冠心病10余年，平时时有胸闷不适，无胸痛心悸现象，时有不适时服用麝香保心丸有所好转。患者50年前曾有左乳房肿块手术史。体检：体温 37.1℃，呼吸18次/分，血压141/107mmHg。两肺呼吸音清，心率114次/分，心律不齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：2021-12-14中心医院：6.51mmol/L。糖化血红蛋白：5.50%，低密度脂蛋白：2.04mmol/L，甘油三酯：0.64mmol/L，高密度脂蛋白：1.27mmol/L，总胆固醇：3.34mmol/L。Ca：2.18mmol/L，Cl：100.25mmol/L，K：3.35mmol/L，Na：141.63mmol/L，P：0.95mmol/L。A/G：1.27，白蛋白：40.77g/L，谷丙转氨酶：7.45u/L，间接胆红素：10.18umol/L，碱性磷酸酶（ALP）：81.18u/L，总胆红素：18.97umol/L，总蛋白：72.78g/L。球蛋白：32.01g/L。直接胆红素：8.79umol/L。肌酐：53.63umol/L，尿素氮：3.58mmol/L，尿酸：290.85umol/L，白细胞：138.00u/L，管型：0u/L，尿胆原：+，葡萄糖：-mmol/L，酸碱度：9.0，酮体：-，亚硝酸盐：+，隐血：+。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'331', N'1053', N'患者家属', N'基本可信', N'胡新志', N'2018-11-22 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'335', N'1055', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'340', N'1065', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'371', N'1093', N'其他', N'基本可靠', N'周婷', N'2019-07-25 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'375', N'1097', N'患者家属', N'基本可靠', N'张丁', N'2019-11-14 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'415', N'1137', N'患者家属', N'基本可靠', N'胡新志', N'2021-11-12 00:00:00.000', NULL, NULL, N'', N'', N'戎苏彭，男，86岁，反复头晕头痛近四十余年。患者四十余年前无诱因反复出现头晕头痛不适，无恶心、呕吐，无视物旋转，无黑朦，无胸闷心悸不适，在闵行中心医院就诊，诊断“高血压病”，长期口服氨氯地平片等药物，血压维持在110-170/70-100mmHg之间。一年前无诱因突发意识丧失模糊、反应迟钝、左侧肢体乏力，无恶心，无抽搐，到闵行中心医院就诊，诊断：“脑血管病、脑梗死、高血压”予以住院治疗，病情好转出院，未遗留肢体功能障碍。近一年来因年事已高，反应迟钝，记忆力减退，智能减退，个人生活不能自理，于20211112自愿入住我院。患者一般情况尚可，轮椅推入病房，对答尚切题，智能减退，查体合作，饮食可、睡眠尚可，小便正常。否认肝炎结核等传染病史。冠心病史5年，脑梗死史1年，便秘史1年。体检：体温 36.3℃，脉搏97次/分，呼吸20次/分，血压131/80mmHg。两肺呼吸音粗，心率97次/分，心律不齐，无腹壁紧张，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。双下肢轻度浮肿生化:葡萄糖：7.7mmol/L（2021-10-25华东医院）。糖化血红蛋白：7.50%（2021-10-25华东医院），低密度脂蛋白：4.27mmol/L（2021-10-25华东医院），甘油三酯：1.14mmol/L，高密度脂蛋白：1.37mmol/L，总胆固醇：5.8mmol/L。Ca：2.3mmol/L，Cl：96.2mmol/L，K：4.68mmol/L，Na：135mmol/L，P：1.2mmol/L（2021-10-25华东医院）。白蛋白：42g/L，谷丙转氨酶：19u/L，间接胆红素：7.2umol/L，碱性磷酸酶（ALP）：152u/L（2021-10-25华东医院），总胆红素：14.4umol/L，总蛋白：65g/L。直接胆红素：7.2umol/L。肌酐：102umol/L，尿素氮：9.7mmol/L，尿酸：444umol/L，白细胞：3-5u/L（2021-10-25华东医院），隐血：+++。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'420', N'1143', N'患者本人及家属', N'可信', N'庄秋丽', N'2022-01-20 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'422', N'1147', N'患者本人及家属', N'可信', N'庄秋丽', N'2022-02-24 00:00:00.000', NULL, NULL, NULL, NULL, N'朱秀英，女，91岁，两下肢行走不利1年左右。患者自去年3月份上厕所时跌倒，致两侧部分肋骨骨折，住院卧床8个月，之后两下肢出现无力，行走不利，目前扶手尚能缓慢行走，基本生活不能自理。昨日松江中心医院CT检查：双侧基底节区、放射冠区可疑腔隙灶，老年脑改变。患者原有高血压病史10余年，最高血压160/90mmHg，以往曾服用吲达帕胺片，最近因为血压偏低，故目前未服药，平时无头晕头痛等不适。患者原有肾功能不全4-5年，肾结石史7-8年，曾服药治疗，目前无不适症状，昨日松江中心医院检查提示肌酐高。昨日松江中心医院CT检查：右肺上叶斑片影，右侧微少量胸腔积液，目前患者无咳嗽咳痰等不适。患者目前一般情况可，无发热，纳食可，两便无异常，睡眠可。
高血压病10余年，肾功能不全4-5年，肾结石7-8年，部分肋骨陈旧性骨折1年左右。否认曾有糖尿病、冠心病、慢性支气管炎等其他慢性病史。体检：体温 36.1℃，脉搏79次/分，呼吸20次/分，血压103/60mmHg。两肺呼吸音粗。79次/分，心律齐。四肢有水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为3级。两下肢膝关节以下水肿，压之凹陷。生化:葡萄糖：4.79mmol/L。糖化血红蛋白：5.00%，低密度脂蛋白：1.96mmol/L，甘油三酯：0.79mmol/L，高密度脂蛋白：1.10mmol/L，总胆固醇：3.48mmol/L。Ca：2.13mmol/L，Cl：103.43mmol/L，K：4.06mmol/L，Na：137.89mmol/L，P：0.89mmol/L。A/G：0.82，白蛋白：34.23g/L，谷丙转氨酶：3.02u/L，间接胆红素：6.07umol/L，碱性磷酸酶（ALP）：124.72u/L，总胆红素：9.33umol/L，总蛋白：75.90g/L。球蛋白：41.67g/L。直接胆红素：3.26umol/L。肌酐：112.10umol/L，尿素氮：5.75mmol/L，尿酸：479.30umol/L，白细胞：90u/L，管型：0u/L，尿胆原：++，葡萄糖：_mmol/L，酸碱度：6.0，酮体：_，上皮细胞：9.0u/L，亚硝酸盐：_，隐血：+。心电图：正常心电图X线胸片：(缺)。B超：肝脏囊肿，双肾弥漫性病变、右肾囊肿，脾肿大、脾内多发实性结节，胆囊术后未显示，肝内外胆管未见明显扩张，胰腺显示不清。CT：右肺上叶斑片影，两肺散在小结节，主动脉、冠状动脉壁部分钙化，右侧微少量胸腔积液，脾肿大，左肾囊性灶，胸7、胸11-12椎体变扁。两侧部分肋骨陈旧性骨折，双侧基底节区、放射冠区可疑腔隙灶，老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'426', N'1150', N'患者家属', N'', N'袁纯兰', N'2022-03-03 00:00:00.000', NULL, NULL, N'', N'', N'冯春荣，男，86岁，记忆力减退六年，加重一年余。患者六年前发现患者记忆力减退，2020年10月起出现言语障碍，不会穿衣服，不认识家人等表现，在瑞金医院就诊，诊断为“阿尔茨海默病”，给予长期服用盐酸美金刚片，目前一般情况稳定。患者既往便秘病史，给予龙荟丸治疗。因生活不能自理于2022年3月3号入住我福利院。发病以来，患者精神可，无发热，无咳嗽气急，无呕吐腹泻，饮食睡眠可。患者高血压病史10年，曾间断服用贝那普利控制血压，一年前因监测血压正常已停药。帕金森病6年余，慢支病史15余年，阑尾炎术后35年余。体检：体温 36.5℃，脉搏68次/分，呼吸20次/分，血压124/67mmHg。两肺呼吸音清，心率68次/分，心律齐，腹软，无压痛，四肢活动可，左下肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：5.3mmol/L（2022.3.1松江中心医院）。碱性磷酸酶（ALP）：145.97u/L。心电图：正常心电图（2022.3.1松江中心医院）B超：肝脏囊肿、肝内钙化灶 双肾囊肿、左肾小钙化灶 胆囊、胰腺、脾脏未见明显占位。CT：慢性支气管炎，主动脉、冠状动脉硬化、左侧少量胸腔积液，食管裂孔疝。头颅CT:双侧放射冠区腔隙灶，老年脑改变')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'429', N'1153', N'患者本人及家属', N'可信', N'袁纯兰', N'2022-07-05 00:00:00.000', NULL, NULL, NULL, NULL, N'陈婉官，女，83岁，反复发作头晕头痛15年余。患者15年前无明显诱因下出现头晕不适，于松江区中心医院就诊时诊断为高血压病，最高血压160/95mmHg，规律服用非洛地平片后血压趋于稳定，后定期在当地社区卫生服务中心配药，目前血压控制可，头晕不适好转。患者曾有慢性胃炎、胃溃疡病史12余年，上腹不适，嗳气等发作时服用法莫替丁、奥美拉唑肠溶胶囊、雷贝拉唑肠溶片后症状缓解。患者因焦虑状态服用氟哌噻吨美利曲辛片，目前稳定。睡眠障碍服用艾司唑仑片助眠。既往焦虑状态约5年，双眼白内障4年余，睡眠障碍4年余。体检：体温 36.1℃，脉搏72次/分，呼吸18次/分，血压136/74mmHg。两肺呼吸音清，无干啰音、湿啰音，心率72次/分，心律齐，腹软，无压痛，无反跳痛。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：mmol/L。Rbc：4.32×10<sup>12</sup>/L，Hb：110g/L，Wbc：6.88×10<sup>9</sup>/L，中性粒比例：70.4%，Plt：268×10<sup>9</sup>/L（2022.6.24松江区中心医院）。Cl：104.3mmol/L（2022.6.24松江区中心医院），K：3.65mmol/L，Na：139mmol/L，白蛋白：34g/L，谷丙转氨酶：20.3u/L，总胆红素：小于1.4umol/L，直接胆红素：小于0.9umol/L（2022.6.24松江区中心医院）。肌酐：53umol/L，尿素氮：5.67mmol/L，尿酸：239umol/L（2022.6.24松江区中心医院），心电图：正常心电图（2022.6.20松江区中心医院）X线胸片：(缺)。B超：血吸虫肝病、肝脏实性占位，脾脏钙化灶，子宫前方实性占位，双侧胸腔少量积液，，盆腹腔少量积液，宫腔积液（2022.6.20松江区中心医院）CT：右额叶软化灶可能，老年脑改变（2022.7.2松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'430', N'1154', N'患者本人及家属', N'可靠', N'袁纯兰', N'2022-08-05 00:00:00.000', NULL, NULL, N'', N'', N'李秀，女，90岁，多饮、多食、多尿20余年。患者20余年前无明显诱因下出现多饮、多食、多尿，在松江区中心原因就诊，诊断为糖尿病。给予二甲双胍口服控制血糖，后家属定期在社区卫生服务中心配药，，偶有监测血糖，提示略偏高，2021年5月因监测血糖20mmol/l，给予调整为门冬胰岛素和地特胰岛素皮下注射控制血糖，后监测血糖尚可，具体数值不详。发病以来患者无发热，无胸闷气急等，饮食睡眠可，大便如常。
患者高血压病口服缬沙坦氨氯地平片控制血压，目前控制可；冠心病支架植入术后长期口服氯吡格雷；慢性支气管炎长期口服复方甲氧那明片；睡眠障碍长期口服艾司唑仑。否认肝炎、结核病史。青霉素过敏史。高血压病、冠心病支架植入术后、慢性支气管炎、睡眠障碍等病史。体检：体温 36.8℃，脉搏76次/分，呼吸20次/分，血压134/65mmHg。两肺呼吸音清，未及干、湿啰音，心率76次/分，心律齐，腹软，无压痛。生化:葡萄糖：10.26mmol/L。Rbc：4.03×10<sup>12</sup>/L，Hb：118g/L，Wbc：2.95×10<sup>9</sup>/L，中性粒比例：63%，淋巴细胞比例：9.5%，Plt：152×10<sup>9</sup>/L，尿隐血：+。心电图：窦性心律，T波改变。B超：左肾囊肿，左肾钙乳症，肝脏、胆囊、胰腺、脾脏、右肾未见明显占位。CT：双侧额顶叶萎缩，老年脑改变，心脏增大，主动脉及冠状动脉硬化，右侧叶积液，右肺中叶及左肺上叶纤维灶，胆囊泥沙样结石。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'431', N'1155', N'患者本人及家属', N'可信', N'袁纯兰', N'2022-08-05 00:00:00.000', NULL, NULL, NULL, NULL, N'金九兴，男，92岁，时有头晕头痛30余年。患者30余年前无明显诱因下出现头晕头痛不适，于松江区中心医院就诊，诊断为高血压病，最高血压160/95mmHg，规律服用苯磺酸氨氯地平片后血压趋于稳定，后间断在当地社区卫生服务中心配药和服药，目前未服药，无头晕头痛不适。发病以来患者无胸闷气急，无恶心呕吐等，饮食睡眠可，二便如常。脑梗死病史3年。慢性支气管炎、肺气肿多年，长期吸氧治疗。否认糖尿病、冠心病等。体检：体温 36.8℃，脉搏69次/分，呼吸20次/分，血压173/75mmHg。两肺呼吸音清，无干啰音、湿啰音。心率69次/分，心律齐，腹软，无压痛，左上肢肌力为4级,左下肢肌力为4级,右上肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：9.65mmol/L，Rbc：3.13×10<sup>12</sup>/L，Hb：109g/L，Wbc：6.1×10<sup>9</sup>/L，中性粒比例：58.3%，淋巴细胞比例：27.2%，Plt：106×10<sup>9</sup>/L。糖化血红蛋白：5.9%，肌酐：140.28umol/L，尿素氮：8.48mmol/L，尿酸：573.01umol/L。心电图：窦性心律，STT改变。B超：血吸虫肝病，肝囊肿，双肾偏小，右肾囊肿。CT：两侧放射冠区多发腔隙灶，老年脑改变，慢性支气管炎，肺气肿，右肺中叶，两肺下叶少许纤维灶，心脏增大，主动脉及冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'433', N'1157', N'患者本人', N'可信', N'庄秋丽', N'2022-08-29 00:00:00.000', NULL, NULL, NULL, NULL, N'张惠娟，女，84岁，右膝关节酸痛10余年，行走欠稳欠利7-8年。患者10余年前开始右膝关节酸痛不适，平时发作严重时常服用消炎止痛药，7-8年前开始行走欠稳欠利，目前患者独自站立不稳，拄拐尚能缓慢行走，酸痛时常发作，严重时曾有关节积液，曾于市区、松江等多家医院就诊过。
患者原有高血压病7-8年，常服用苯磺酸左氨氯地平片降压，据说血压控制理想，平时无头晕头痛等不适，最高血压（患者及家属无法说清）不高。
患者原有吸血虫肝病30余年，右膝关节炎10余年，高血压病7-8年。否认曾有冠心病、肿瘤、慢性支气管炎等其他慢性病。
体检：体温 36.6℃，脉搏90次/分，呼吸20次/分，血压149/79mmHg。:神清，精神可，巩膜清，结膜无异常，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率90次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，肠鸣音无异常，两下肢无水肿，左上肢肌力为5级，左下肢肌力为5级，右上肢肌力为5级，右下肢肌力为3级。生化:葡萄糖：5.26mmol/L。Rbc：4.06×10<sup>12</sup>/L，Hb：117g/L，Wbc：7.39×10<sup>9</sup>/L，中性粒比例：66.60%，淋巴细胞比例：25.40%，Plt：315×10<sup>9</sup>/L。糖化血红蛋白：6.90%，低密度脂蛋白：4.66mmol/L，甘油三酯：1.09mmol/L，高密度脂蛋白：1.54mmol/L，总胆固醇：6.45mmol/L。Ca：2.23mmol/L，Cl：105.84mmol/L，K：3.91mmol/L，Na：140.92mmol/L，P：1.33mmol/L。A/G：1.39，白蛋白：41.18g/L，谷丙转氨酶：9.73u/L，间接胆红素：5.58umol/L，碱性磷酸酶（ALP）：83.03u/L，总胆红素：8.02umol/L，总蛋白：70.90g/L。球蛋白：29.72g/L。直接胆红素：2.44umol/L。肌酐：62.24umol/L，尿素氮：7.3mmol/L，尿酸：296.72umol/L，白细胞：67.40u/L，管型：0.27u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：6.0，酮体：阴性（-），上皮细胞：18.90u/L，亚硝酸盐：阴性（-），隐血：2+。心电图：窦性心律，ST段变化，II、III、aVF、V4~V6水平压低0.05-0.1mv。B超：肝弥漫性改变，血吸虫肝。肝内钙化灶形成。脾内钙化灶形成。胆囊、胰腺、双肾未见明显异常。CT：桥脑右份及双侧基底节区腔隙灶，老年脑改变，左肺下叶少许纤维灶，心脏增大，主动脉及双侧冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'435', N'1159', N'患者家属', N'基本可信', N'周婷', N'2022-09-05 00:00:00.000', NULL, NULL, NULL, NULL, N'姜长海，男，80岁，头晕头痛反复发作二十余年。患者原有高血压史二十余年，最高血压值190/130mmHg，目前长期服用非洛地平缓释片、缬沙坦胶囊控制血压，血压控制较稳定。十余年前患有脑梗及右侧小脑后听神经良性肿瘤，经华山医院就诊后予保守治疗至今，时有出现三叉神经疼痛等不适症状，长期服用卡马西平及甲磺酸倍他司汀片予对症治疗。目前因患者年事渐高，家中无人照顾，于2022年09月04号入住本院。患者目前精神、饮食、睡眠均可，大小便正常。 
 
脑梗、慢性支气管炎、右侧小脑听神经良性肿瘤体检：体温 36.5℃，脉搏77次/分，呼吸20次/分，血压147/88mmHg。两肺呼吸音清，无干啰音、无哮鸣音。心率77次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6mmol/L mmol/L（松江区中心医院）。Rbc：4.77 ×10<sup>12</sup>/L（松江区中心医院），Hb：145g/L，Wbc：5.58×10<sup>9</sup>/L，中性粒比例：68.5%，淋巴细胞比例：18.5%，Plt：201×10<sup>9</sup>/L。C反应蛋白：4.87 MG/L（松江区中心医院），糖化血红蛋白：6.1 %（松江区中心医院），低密度脂蛋白：2.23mmol/L，甘油三酯：1.09 mmol/L（松江区中心医院），高密度脂蛋白：0.91mmol/L，总胆固醇：3.47mmol/L。Ca：2.06mmol/L，Cl：97.81mmol/L，K：4.07 mmol/L（松江区中心医院），Na：134.99mmol/L，P：1.01mmol/L。A/G：1.41，白蛋白：42.39g/L，谷丙转氨酶：9.05 u/L（松江区中心医院），间接胆红素：4.45umol/L，碱性磷酸酶（ALP）：104.39u/L，总胆红素：8.2umol/L，总蛋白：72.55g/L。球蛋白：30.16g/L。直接胆红素：3.75umol/L。肌酐：75.15umol/L，尿素氮：4.66 mmol/L（松江区中心医院），尿酸：183.01umol/L，白细胞：（-）u/L，管型：0.00u/L，尿胆原：（-） （松江区中心医院），葡萄糖：（-）mmol/L，酸碱度：7.0，酮体：（-），上皮细胞：0.00u/L，亚硝酸盐：（-），隐血：（-）。心电图：窦性心律、Ⅰ度房室传导阻滞X线胸片：(缺)。B超：1.肝脏多发囊肿2.胰腺体部囊性结节3.双肾囊肿、左肾结石CT：1.右侧桥小脑角区占位2.双侧基底节区及半卵圆中心多发腔梗3.老年脑、脑白质改变4.双肺胸膜下坠积性改变5.右肺中叶小结节6.双肺小钙化灶7.心脏增大、主动脉及冠状动脉硬化8.双侧胸膜增厚、钙化')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'437', N'1160', N'患者家属', N'基本可信', N'周婷', N'2022-09-11 00:00:00.000', NULL, NULL, NULL, NULL, N'鲍祖庭，男，71岁，记忆力减退，答非所问，生活不能自理二年余。患者近两年来记忆力减退明显，答非所问，除了可自行拿碗吃饭，其他自理能力均丧失。近一月去松江区精神卫生中心就诊后，诊断为：血管性痴呆。患者自起病来精神、饮食、睡眠可，夜尿频繁（原有前列腺增生史，长期用药）。因目前家中无人照顾于2022-09-08入住本院。二尖瓣狭窄伴主动脉瓣关闭不全、风湿性心脏病、主动脉瓣生物瓣膜置换术、心房颤动、脑梗死个人体检：体温 37℃，脉搏89次/分，呼吸20次/分，血压100/64mmHg。两肺呼吸音清，无干啰音；心率89次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.17mmol/L（松江区中心医院）
。Rbc：4.38×10<sup>12</sup>/L（松江区中心医院），Hb：135g/L，Wbc：4.51×10<sup>9</sup>/L，中性粒比例：64.9%，淋巴细胞比例：21.3%，Plt：159×10<sup>9</sup>/L。C反应蛋白：未检MG/L，糖化血红蛋白：5.9%（松江区中心医院），低密度脂蛋白：3.14mmol/L，甘油三酯：0.80mmol/L（松江区中心医院），高密度脂蛋白：1.33mmol/L，总胆固醇：4.56mmol/L。Ca：2.32mmol/L，Cl：100.51mmol/L，K：4.27mmol/L（松江区中心医院），Na：139.3mmol/L，P：1.19mmol/L。A/G：1.97，白蛋白：47.88g/L，谷丙转氨酶：12.85u/L（松江区中心医院），间接胆红素：11.32umol/L，碱性磷酸酶（ALP）：77.97u/L，总胆红素：17.67umol/L，总蛋白：72.15g/L。球蛋白：24.27g/L。直接胆红素：6.35umol/L。肌酐：83.1umol/L，尿素氮：6.84mmol/L（松江区中心医院），尿酸：302.17umol/L，白细胞：7u/L，管型：0u/L，尿胆原：-（松江区中心医院），葡萄糖：-mmol/L，酸碱度：5.0，酮体：-，上皮细胞：3u/L，亚硝酸盐：-，隐血：-。心电图：心房颤动伴快速心室率、频发室早、T波改变X线胸片：(缺)。B超：双肾囊肿（松江区中心医院）CT：1.左肺上叶纤维灶 2.心脏增大、主动脉及冠状动脉硬化，心脏起搏器术后改变 3.双侧第7、8前肋陈旧性骨折 4.双侧额颞顶部硬膜下血肿、左侧稍明显 5.左侧枕顶叶、右侧额叶、右侧放射冠-基底节区多发软化灶 6.老年脑改变（松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'443', N'1171', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'戴喜柱，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，左下肢残疾，可独立完成洗脸刷牙、进食、行走、如厕等，穿脱衣、洗浴方面需辅助完成。入院前体检提示尿酸偏高，总胆固醇、低密度脂蛋白胆固醇偏高，T波低平。体检：体温 36.9℃，脉搏80次/分，呼吸18次/分，血压136/85mmHg。两肺呼吸音粗，无干湿啰音。心率80次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.75mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：4.54×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：145g/L，Wbc：5.90×10<sup>9</sup>/L，中性粒比例：64.0%，淋巴细胞比例：29.9%，Plt：279×10<sup>9</sup>/L。低密度脂蛋白：5.73mmol/L，甘油三酯：0.73mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：1.70mmol/L，总胆固醇：7.48mmol/L。谷丙转氨酶：16u/L（南京艾迪康医学检验所，2022-10-10），碱性磷酸酶（ALP）：105u/L，总胆红素：9.2umol/L，肌酐：67umol/L，尿素氮：8.00mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：398umol/L，心电图：T波低平。（上海市上农医院，2022-10-10）X线胸片：（-）。（上海市上农医院，2022-10-10）B超：（-）。（上海市上农医院，2022-10-10）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'448', N'1169', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦婉，女，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴等无法独立完成。患者目前服用苯磺酸氨氯地平片、丹参片、阿司匹林肠溶片等。本次入院体检提示脑梗死、肝囊肿、陈旧性肺结核、高血压病。 体检：体温 36.3℃，脉搏82次/分，呼吸20次/分，血压96/70mmHg。两肺呼吸音粗，心率82次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：6.1mmol/L，Rbc：3.86×10<sup>12</sup>/L，Hb：118g/L，Wbc：4.9×10<sup>9</sup>/L，中性粒比例：68.3%，淋巴细胞比例：30.3%，Plt：208×10<sup>9</sup>/L。糖化血红蛋白：5.8%%，低密度脂蛋白：1.91mmol/L，甘油三酯：1.16mmol/L，高密度脂蛋白：1.41mmol/L，总胆固醇：4.96mmol/L。Cl：105.2mmol/L，K：4.58mmol/L，Na：143.5mmol/L，A/G：1.73，白蛋白：41.2g/L，谷丙转氨酶：14u/L，间接胆红素：7.5umol/L，碱性磷酸酶（ALP）：77u/L，总蛋白：65g/L。球蛋白：23.8g/L。直接胆红素：1.2umol/L。肌酐：43umol/L，尿素氮：5mmol/L，尿酸：207umol/L（2022.11.15上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'446', N'1168', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'林勇，男，71岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴等均不能独立完成，完全需要照料。患者目前服用血塞通胶囊、阿司匹林肠溶片、便通胶囊等。本次入院前体检提示脑梗死后遗症、高血压病、脑积水、肾功能异常、肺部感染、足皮肤感染、低蛋白血症、前列腺增生等。体检：体温 36.7℃，脉搏89次/分，呼吸20次/分，血压144/89mmHg。两肺呼吸音粗，心率89次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿，左上肢肌力为3级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.38mmol/L（2022.11.15上海市上农医院）。Rbc：3.9×10<sup>12</sup>/L，Hb：106g/L，Wbc：7.9×10<sup>9</sup>/L，中性粒比例：88.5%，淋巴细胞比例：9.7%，Plt：250×10<sup>9</sup>/L。甘油三酯：0.76mmol/L，总胆固醇：3.96mmol/L（2022.11.15上海市上农医院）。K：4.32mmol/L，白蛋白：27.4g/L，谷丙转氨酶：13u/L，碱性磷酸酶（ALP）：78u/L，肌酐：82umol/L，尿素氮：7mmol/L，尿酸：250umol/L，心电图：窦性心律，T波地平，X线胸片：两肺炎性改变，心影增大，双侧锁骨远端陈旧性骨折，双侧肋骨多发性陈旧性骨折，胸椎侧弯，B超：双肾小结晶，左肾轻度积水，前列腺增生伴钙化（2022.11.15上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'451', N'1189', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦美，女，66岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、洗浴、穿脱衣服、行走、如厕等需要完全辅助。进食可以独立完成。患者目前服用苯海索片、利培酮片。本次入院体检提示窦性心动过缓，血脂偏高。 体检：体温 36.4℃，脉搏89次/分，呼吸20次/分，血压102/75mmHg。两肺呼吸音清，心率89次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.52mmol/L（2022.10.10上海市上农医院）。Rbc：3.75×10<sup>12</sup>/L，Hb：122g/L，Wbc：7.59×10<sup>9</sup>/L，中性粒比例：67.8%，淋巴细胞比例：27.1%，Plt：290×10<sup>9</sup>/L。低密度脂蛋白：3.23mmol/L。甘油三酯：0.64mmol/L，高密度脂蛋白：1.67mmol/L，总胆固醇：5.5mmol/L。谷丙转氨酶：16u/L，碱性磷酸酶（ALP）：64u/L。总胆红素：7umol/L，肌酐：56umol/L，尿素氮：5.92mmol/L，尿酸：177umol/L。心电图：窦性心动过缓，T波改变。。B超：（-）（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'453', N'1191', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦喜，女，80岁，智力低下数十年。  该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、行走、如厕等可以独立完成，洗浴、穿脱衣服需要辅助。目前服用吲达帕胺控制血压。本次入院体检提示高血压病、高脂血症、脂肪肝。 体检：体温 36.4℃，脉搏90次/分，呼吸20次/分，血压148/89mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率90次/分，心律齐，未闻及早搏,腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：5.38mmol/L。Rbc：4.76×10<sup>12</sup>/L，Hb：129g/L，Wbc：5×10<sup>9</sup>/L，中性粒比例：49.4%，淋巴细胞比例：42%，Plt：219×10<sup>9</sup>/L。低密度脂蛋白：4.74mmol/L。甘油三酯：2.45mmol/L，高密度脂蛋白：1.88mmol/L，总胆固醇：7.88mmol/L。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：27u/L，总胆红素：10.6umol/L。肌酐：77umol/L，尿素氮：6.6mmol/L，尿酸：321umol/L。心电图：窦性心律，T波改变。B超：脂肪肝（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'454', N'1192', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦艳，女，67岁，智力低下数十年。  该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似聋哑，日常生活中洗漱、进食、行走、如厕等可以独立完成，洗浴、穿脱衣服需要完全辅助。本次入院体检提示乙型病毒性肝炎、高脂血症。 体检：体温 36.3℃，脉搏68次/分，呼吸20次/分，血压135/78mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率68次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，左上肢肌力为2级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。左上肢残疾。生化:葡萄糖：4.85mmol/L.Rbc：4.31×10<sup>12</sup>/L，Hb：138g/L，Wbc：5.88×10<sup>9</sup>/L，中性粒比例：50.2%，淋巴细胞比例：41.8%，Plt：182×10<sup>9</sup>/L。低密度脂蛋白：3.3mmol/L，甘油三酯：1.22mmol/L，高密度脂蛋白：1.8mmol/L，总胆固醇：5.94mmol/L。谷丙转氨酶：29u/L，碱性磷酸酶（ALP）：112u/L，总胆红素：9.5umol/L。肌酐：56umol/L，尿素氮：5.75mmol/L，尿酸：261umol/L。心电图：正常心电图。B超：（-）（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'455', N'1174', N'其他', N'基本可靠', N'王屹', N'2022-11-30 00:00:00.000', NULL, NULL, N'救助二站', N'', N'韩宝明，男，75岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，并聋哑，日常生活中可独立完成洗脸刷牙、进食、穿脱衣、行走、如厕等，洗浴方面需协助完成。入院前体检提示甘油三酯偏高、Ⅰ度房室传导阻滞、左下肺类圆形高密度影。患者目前精神状态可，食欲、大小便、睡眠较正常。乙肝病史数十年体检：体温 36.7℃，脉搏96次/分，呼吸18次/分，血压164/95mmHg。两肺呼吸音清，96次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。生化:葡萄糖：4.98mmol/L。Rbc：5.67×10<sup>12</sup>/L，Hb：168g/L，Wbc：7.08×10<sup>9</sup>/L，中性粒比例：71.6%，淋巴细胞比例：22.1%，Plt：172×10<sup>9</sup>/L。低密度脂蛋白：2.13mmol/L，甘油三酯：4.21mmol/L，高密度脂蛋白：0.98mmol/L，总胆固醇：4.73mmol/L。谷丙转氨酶：46u/L，总胆红素：7.3umol/L，肌酐：75umol/L，尿素氮：5.58mmol/L，尿酸：246umol/L。心电图：1、窦性心律2、Ⅰ度房室传导阻滞X线胸片：左下肺类圆形高密度影（23mm），占位待排B超：正常')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'459', N'1194', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政才，男，71岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似视听障碍，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴需要辅助。本次入院体检提示窦性心动过缓，血脂偏高。体检：体温 36.2℃，脉搏76次/分，呼吸20次/分，血压161/100mmHg。两肺呼吸音清，无干啰音、湿啰音，心率76次/分，心律齐，未闻及早搏,腹软，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。跛脚。生化:葡萄糖：5.03mmol/L（2022.10.10上海市上农医院）。Rbc：5.17×10<sup>12</sup>/L，Hb：152g/L，Wbc：8.13×10<sup>9</sup>/L，中性粒比例：51.7%，淋巴细胞比例：39%，Plt：277×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：4.08mmol/L，甘油三酯：0.69mmol/L，高密度脂蛋白：1.3mmol/L，总胆固醇：5.53mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：14u/L，碱性磷酸酶（ALP）：109u/L，总胆红素：10.1umol/L（2022.10.10上海市上农医院），肌酐：72umol/L，尿素氮：5.51mmol/L，尿酸：295umol/L（2022.10.10上海市上农医院），心电图：窦性心动过缓，STT改变（2022.10.10上海市上农医院），B超：（-）（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'465', N'1178', N'其他', N'基本可靠', N'周婷', N'2022-11-30 00:00:00.000', NULL, NULL, N'', N'', N'沈灯英，女，73岁，智力低下数十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中可独立完成洗脸刷牙、进食、行走；穿脱衣、洗浴、如厕等方面需辅助完成。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高。患者目前精神状态可，食欲、大小便、睡眠较正常。体检：体温 36.9℃，脉搏68次/分，呼吸20次/分，血压160/88mmHg。两肺呼吸音清，无干啰音、心率68次/分，心律齐，未闻及早搏，腹部无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.56   mmol/L（上海市上农医院）。Rbc：4.01  ×10<sup>12</sup>/L（上海市上农医院），Hb：121g/L，Wbc：9.06×10<sup>9</sup>/L，中性粒比例：73.3%，淋巴细胞比例：21.6%，Plt：230×10<sup>9</sup>/L。低密度脂蛋白：5.28mmol/L，甘油三酯：1.32  mmol/L（上海市上农医院），高密度脂蛋白：1.83mmol/L，总胆固醇：7.71mmol/L。谷丙转氨酶：11 u/L（上海市上农医院），碱性磷酸酶（ALP）：55u/L，总胆红素：6.0umol/L，肌酐：70umol/L，尿素氮：6.49  mmol/L（上海市上农医院），尿酸：183umol/L，心电图：正常心电图X线胸片：无特殊异；B超无特殊异常。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'466', N'1177', N'其他', N'基本可靠', N'周婷', N'2022-11-30 00:00:00.000', NULL, NULL, N'救助二站', N'', N'任明国，男，67岁，智力低下数十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下伴语言障碍。日常生活中可独立完成洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高；心电图检查：窦性心动过缓、T波高尖；B超提示：胆囊炎可能？患者目前精神状态可，食欲、大小便、睡眠较正常。体检：体温 37.2℃，脉搏97次/分，呼吸20次/分，血压128/87mmHg。两肺呼吸音清，无干啰音、无哮鸣音；心率97次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.52 mmol/L（上海市上农医院）。Rbc：5.69  ×10<sup>12</sup>/L（上海市上农医院），Hb：156g/L，Wbc：5.2×10<sup>9</sup>/L，中性粒比例：46%，淋巴细胞比例：46%，Plt：237×10<sup>9</sup>/L。低密度脂蛋白：4.39mmol/L，甘油三酯：0.84 mmol/L（上海市上农医院），高密度脂蛋白：1.61mmol/L，总胆固醇：6.31mmol/L。谷丙转氨酶：24 u/L（上海市上农医院），碱性磷酸酶（ALP）：70u/L，总胆红素：10.4umol/L，肌酐：80umol/L，尿素氮；X线胸片：（-）B超：胆囊壁毛糙。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'476', N'1217', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆巧，女，76岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、洗浴、行走、如厕等方面能够独立完成。入院前体检提示血脂偏高。体检：体温 37.3℃，脉搏82次/分，呼吸20次/分，血压130/74mmHg。两肺呼吸音清，无干湿啰音。心率82次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.02mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.81×10<sup>12</sup>/L，Hb：140g/L，Wbc：6.35×10<sup>9</sup>/L，中性粒比例：62.7%，淋巴细胞比例：21.9%，Plt：225×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：2.36mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：1.83mmol/L，高密度脂蛋白：1.73mmol/L，总胆固醇：4.92mmol/L。谷丙转氨酶：23u/L，碱性磷酸酶（ALP）：162u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：11.5umol/L，肌酐：46umol/L，尿素氮：6.44mmol/L，尿酸：193umol/L（南京艾迪康医学检验所，2022-10-12），心电图：正常（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'477', N'1213', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民原，男，79岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、如厕等方面能够独立完成，洗澡需要协助完成，行走缓慢，能听懂话语，但无交流。体检：体温 36.9℃，脉搏72次/分，呼吸20次/分，血压108/68mmHg。两肺呼吸音清，无干湿啰音。心率72次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.11mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.20×10<sup>12</sup>/L，Hb：129g/L，Wbc：5.52×10<sup>9</sup>/L，中性粒比例：66.3%，淋巴细胞比例：24.1%，Plt：224×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：3.40mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：0.84mmol/L，高密度脂蛋白：1.72mmol/L，总胆固醇：5.46mmol/L。谷丙转氨酶：11u/L，碱性磷酸酶（ALP）：59u/L，总胆红素：14.5umol/L，肌酐：83umol/L，尿素氮：7.26mmol/L，尿酸：510umol/L（南京艾迪康医学检验所，2022-10-12），心电图：窦性心律不齐，ST段改变。X线胸片：正常（上海市上农医院，2022-10-12）B超：肝多发囊肿，胆囊未显示。（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'481', N'1251', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民宁，男，63岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。患者因高血压病、糖尿病长期口服硝苯地平缓释片、阿卡波糖片、二甲双胍缓释片、格列美脲片治疗中。体检：体温 37.1℃，脉搏105次/分，呼吸20次/分，血压106/74mmHg。两肺呼吸音清，无干湿啰音。心率105次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：10.34mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：5.48×10<sup>12</sup>/L，Hb：162g/L，Wbc：7.99×10<sup>9</sup>/L，中性粒比例：67.7%，淋巴细胞比例：23.6%，Plt：363×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：3.58mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：2.86mmol/L，高密度脂蛋白：1.23mmol/L，总胆固醇：5.69mmol/L。谷丙转氨酶：39u/L，碱性磷酸酶（ALP）：67u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：9.0umol/L，肌酐：70umol/L，尿素氮：4.21mmol/L，尿酸：292umol/L（南京艾迪康医学检验所，2022-10-12），心电图：正常（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：脂肪肝，胆囊未显示。（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'484', N'1195', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政康，男，74岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、行走、如厕等可以独立完成，穿脱衣服、洗浴需要辅助。患者目前服用吲达帕胺片控制血压。本次入院体检心电图提示T波地平。 高血压病史数年。体检：体温 36.3℃，脉搏109次/分，呼吸20次/分，血压106/89mmHg。两肺呼吸音粗，心率109次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿生化:葡萄糖：5.43mmol/L（2022.10.10上海市上农医院）。Rbc：4.55×10<sup>12</sup>/L，Hb：140g/L，Wbc：6.14×10<sup>9</sup>/L，中性粒比例：54.5%，淋巴细胞比例：32.7%，Plt：241×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.51mmol/L，甘油三酯：0.81mmol/L，高密度脂蛋白：1.74mmol/L，总胆固醇：4.73mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：81u/L，总胆红素：7.7umol/L（2022.10.10上海市上农医院），肌酐：51umol/L，尿素氮：8.26mmol/L，尿酸：208umol/L（2022.10.10上海市上农医院），心电图：窦性心律，T波地平（2022.10.10上海市上农医院）X线胸片：（-）B超：（-）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'485', N'1196', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政权，男，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示贫血。 体检：体温 37.3℃，脉搏82次/分，呼吸20次/分，血压121/74mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率82次/分，心律齐，发热，无压痛，无反跳痛，四肢无水肿，左下肢畸形。生化:葡萄糖：4.56mmol/L（2022.10.10上海市上农医院）。Rbc：4.6×10<sup>12</sup>/L，Hb：106g/L，Wbc：4.39×10<sup>9</sup>/L，中性粒比例：48.4%，淋巴细胞比例：41.9%，Plt：424×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.61mmol/L，甘油三酯：0.47mmol/L，高密度脂蛋白：1.86mmol/L，总胆固醇：5.02mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：24u/L，碱性磷酸酶（ALP）：72u/L，总胆红素：8.4umol/L（2022.10.10上海市上农医院），肌酐：67umol/L，尿素氮：5.86mmol/L，尿酸：185umol/L（2022.10.10上海市上农医院），心电图：正常心电图（2022.10.10上海市上农医院）X线胸片：（-）（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'488', N'1208', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民权，男，66岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。体检：体温 36.5℃，脉搏89次/分，呼吸20次/分，血压121/85mmHg。两肺呼吸音清，无干湿啰音。心率89次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.83mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.61×10<sup>12</sup>/L，Hb：140g/L，Wbc：6.03×10<sup>9</sup>/L，中性粒比例：52.9%，淋巴细胞比例：37.7%，Plt：244×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：3.46mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：0.67mmol/L，高密度脂蛋白：1.79mmol/L，总胆固醇：5.74mmol/L。谷丙转氨酶：20u/L，碱性磷酸酶（ALP）：75u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：13.2umol/L，肌酐：100umol/L，尿素氮：6.67mmol/L，尿酸：260umol/L（南京艾迪康医学检验所，2022-10-12），心电图：窦性心律，左心室高电压。（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：肝多发囊肿（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'489', N'1199', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政学，男，66岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似听力障碍，日常生活中洗漱、洗浴、进食、穿脱衣服、行走、如厕等可以独立完成。患者目前不服药。本次入院体检提示右侧腹股沟疝、胆囊未显示。 体检：体温 36.7℃，脉搏93次/分，呼吸20次/分，血压134/89mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率93次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：5.27mmol/L（2022.10.10上海市上农医院）。Rbc：5.33×10<sup>12</sup>/L，Hb：166g/L，Wbc：7.27×10<sup>9</sup>/L，中性粒比例：65.7%，淋巴细胞比例：23.3%，Plt：103×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：3.19mmol/L，甘油三酯：0.57mmol/L，高密度脂蛋白：1.19mmol/L，总胆固醇：4.51mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：31u/L，碱性磷酸酶（ALP）：65u/L，总胆红素：28.5umol/L（2022.10.10上海市上农医院），肌酐：74umol/L，尿素氮：4.72mmol/L，尿酸：258umol/L（2022.10.10上海市上农医院），心电图：窦性心律，左前分支阻滞，T波地平（2022.10.10上海市上农医院）X线胸片：纵膈增宽，左锁骨下多发钙化灶（2022.10.10上海市上农医院）B超：胆囊未显示（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'491', N'1253', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'493', N'1245', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'497', N'1246', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆琴，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等需要协助。体检：体温 36.8℃，脉搏93次/分，呼吸20次/分，血压135/84mmHg。两肺呼吸音粗，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。93次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。生化:葡萄糖：5.16mmol/L（2022.10.14上海市上农医院）。Rbc：4.45×10<sup>12</sup>/L，Hb：135g/L，Wbc：6.09×10<sup>9</sup>/L，中性粒比例：72.1%，淋巴细胞比例：21.1%，Plt：237×10<sup>9</sup>/L（2022.10.14上海市上农医院）。低密度脂蛋白：2.8mmol/L，甘油三酯：0.78mmol/L，高密度脂蛋白：1.46mmol/L，总胆固醇：4.83mmol/L（2022.10.14上海市上农医院）。谷丙转氨酶：11u/L，碱性磷酸酶（ALP）：79u/L，总胆红素：11.1umol/L（2022.10.14上海市上农医院），肌酐：65umol/L，尿素氮：6.05mmol/L，尿酸：235umol/L（2022.10.14上海市上农医院），心电图：正常心电图（2022.10.14上海市上农医院）X线胸片：（-）（2022.10.14上海市上农医院）B超：（-）（2022.10.14上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'498', N'1200', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'徐连青0033，女，71岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食能够独立完成，洗脸刷牙、穿脱衣、洗浴、如厕等方面需要帮助。患者因脑梗死目前口服阿司匹林肠溶片。入院前体检提示血糖偏高、高脂血症。体检：体温 37.4℃，脉搏96次/分，呼吸20次/分，血压131/82mmHg。两肺呼吸音粗。心率96次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为4级,右下肢肌力为3级。生化:葡萄糖：6.24mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.36×10<sup>12</sup>/L，Hb：134g/L，Wbc：6.80×10<sup>9</sup>/L，中性粒比例：56.3%，淋巴细胞比例：34.7%，Plt：347×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：3.76mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：1.30mmol/L，高密度脂蛋白：1.47mmol/L，总胆固醇：5.97mmol/L。谷丙转氨酶：10u/L，碱性磷酸酶（ALP）：78u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：10.1umol/L，肌酐：7umol/L，尿素氮：8.46mmol/L，尿酸：224umol/L（南京艾迪康医学检验所，2022-10-14），心电图：正常心电图（上海市上农医院，2022-10-14）X线胸片：左侧肋膈角变钝（上海市上农医院，2022-10-14）B超：胆囊未显示（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'499', N'1233', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'王黄菊0895，女，67岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣、洗漱、进食、行走、如厕能基本自理，能简单交流。曾在金山精神卫生中心住院治疗，长期服用盐酸苯海拉索、盐酸氯丙嗪片。患者目前精神状态可，食欲、大小便、睡眠较正常。 体检：体温 36.6℃，脉搏88次/分，呼吸20次/分，血压115/74mmHg。两肺呼吸音清，88次/分，心律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.87mmol/L（2022-10-14上海市上农医院）。Rbc：4.27×10<sup>12</sup>/L，Hb：122g/L，Wbc：6.99×10<sup>9</sup>/L，中性粒比例：79.9%，淋巴细胞比例：13.5%，Plt：217×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：2.44mmol/L（2022-10-14上海市上农医院），甘油三酯：0.49mmol/L，高密度脂蛋白：1.42mmol/L，总胆固醇：4.35mmol/L。谷丙转氨酶：15u/L，碱性磷酸酶（ALP）：69u/L（2022-10-14上海市上农医院），总胆红素：9.5umol/L，肌酐：60umol/L，尿素氮：6.41mmol/L，尿酸：227umol/L（2022-10-14上海市上农医院），心电图：正常心电图（2022-10-14上海市上农医院）X线胸片：右侧肋膈角变钝（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'501', N'1241', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助', N'', N'宋圆竹0931，女，84岁，智力低下数十年患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑、失明，双下肢残疾，长期坐轮椅。日常生活中洗漱、进食、行走、穿脱衣、洗浴、如厕等均需他人帮助。因高血压长期服用硝苯地平片。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核等传染病史，有梅毒史。体检：体温 36.3℃，脉搏80次/分，呼吸20次/分，血压128/76mmHg。两肺呼吸音清，80次/分，心律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为3级。双下肢残疾、坐轮椅。生化:葡萄糖：4.18mmol/L（2022-10-14上海市上农医院）。Rbc：3.76×10<sup>12</sup>/L，Hb：126g/L，Wbc：6.91×10<sup>9</sup>/L，中性粒比例：66%，淋巴细胞比例：25%，Plt：324×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.07mmol/L（2022-10-14上海市上农医院），甘油三酯：0.83mmol/L，高密度脂蛋白：1.91mmol/L，总胆固醇：5.57mmol/L。谷丙转氨酶：7u/L，碱性磷酸酶（ALP）：90u/L（2022-10-14上海市上农医院），总胆红素：12.5umol/L，肌酐：194umol/L，尿素氮：13.07mmol/L，尿酸：435umol/L（2022-10-14上海市上农医院），心电图：窦性心律 T波低平（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：胆囊显示不清（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'503', N'1220', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆萍0859，女，72岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活穿脱衣、洗漱、进食、行走、如厕等能完全自理，可简单交流。入院体检提示：脊柱侧弯、血吸虫肝病、左室肥大。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核等传染病史。体检：体温 36.9℃，脉搏87次/分，呼吸20次/分，血压117/71mmHg。两肺呼吸音清，87次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.65mmol/L（2022-10-14上海市上农医院）。Rbc：4.11×10<sup>12</sup>/L，Hb：122g/L，Wbc：6.13×10<sup>9</sup>/L，中性粒比例：70.7%，淋巴细胞比例：21.2%，Plt：202×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.02mmol/L（2022-10-14上海市上农医院），甘油三酯：1.04mmol/L，高密度脂蛋白：1.51mmol/L，总胆固醇：5.14mmol/L。谷丙转氨酶：9u/L，碱性磷酸酶（ALP）：97u/L（2022-10-14上海市上农医院），总胆红素：9.7umol/L，肌酐：54umol/L，尿素氮：6.79mmol/L，尿酸：219umol/L（2022-10-14上海市上农医院），心电图：窦性心律 左室肥大（2022-10-14上海市上农医院）X线胸片：心影增大 脊柱侧弯（2022-10-14上海市上农医院）B超：血吸虫肝（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'536', N'1207', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'李玉萍0235，女，67岁，智力低下数十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下。日常生活中可独立完成洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等。能听懂话语可简单交流。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高。心电图检查：窦性心动过缓；B超提示：脂肪肝、胆结石。患者目前精神状态可，食欲、大小便、睡眠较正常。原有梅毒史。体检：体温 36.3℃，脉搏86次/分，呼吸20次/分，血压124/79mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。86次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.49mmol/L（南京艾迪康医学检验所）。Rbc：4.26×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：128g/L，Wbc：8.31×10<sup>9</sup>/L，中性粒比例：67.8%，淋巴细胞比例：24.2%，Plt：337×10<sup>9</sup>/L。低密度脂蛋白：3.4mmol/L，甘油三酯：2.63mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.21mmol/L，总胆固醇：5.54mmol/L。谷丙转氨酶：14u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：133u/L，总胆红素：7.1umol/L，肌酐：54umol/L，尿素氮：5.3mmol/L（南京艾迪康医学检验所），尿酸：277umol/L，心电图：窦性心动过缓X线胸片：（-）B超：脂肪肝、胆结石。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'539', N'1262', N'患者家属', N'基本可靠', N'胡新志', N'2023-03-02 00:00:00.000', NULL, NULL, N'', N'', N'沈龙根，男，88岁，反复咳嗽咳痰二十余年。患者二十余年前开始反复出现咳嗽咳痰，咳白色泡沫痰，无胸闷气促，冬季、季节交换及受凉时咳嗽咳痰尤为明显，予以对症抗炎、止咳化痰等药物治疗（具体治疗不详）症状可缓解。2022年6月因脑梗塞遗留右侧肢体行动不便，长期卧床，动后有气促现象，居家吸氧治疗。发病以来，情绪尚稳定，体型消瘦，饮食可，夜间睡眠差予以助眠药睡眠改善，排便困难予以通便药物治疗可缓解，尿储留予以长期流质导尿。否认肝炎、结核等传染病史，否认高血压、糖尿病史。体检：体温 36.4℃，脉搏90次/分，呼吸20次/分，血压112/64mmHg。两肺呼吸音粗，90次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 左上肢肌力为4级,左下肢肌力为3级,右上肢肌力为3级,右下肢肌力为3级。生化:Ca：2.12mmol/L，Cl：90.77mmol/L，K：4.05mmol/L，Na：126.2mmol/L，P：1.17mmol/L，A/G：1.23，白蛋白：35.20g/L，谷丙转氨酶：6.77u/L，间接胆红素：5.76umol/L，碱性磷酸酶（ALP）：95.02u/L，总胆红素：10.26umol/L，总蛋白：63.93g/L。球蛋白：28.73g/L。直接胆红素：4.50umol/L。肌酐：55.99umol/L，尿素氮：3.86mmol/L，尿酸：184.31umol/L白细胞：1791u/L，尿胆原：-，酸碱度：7.5，酮体：-，亚硝酸盐：++，隐血：+。心电图：窦性心律 心电图左偏（提示左前分支阻滞）T波改变。B超：肝脏、胆囊、脾脏、双肾未见明显占位，胰脏显示不清。CT：胸部CT：1.慢性支气管炎、肺气肿2.右肺中叶结节3.右肺上叶钙化灶4.心脏增大，心包少量积液，主动脉及冠状动脉硬化5.两侧胸腔少量积液（2023-02-28松江中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'541', N'1264', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-03-23 00:00:00.000', NULL, NULL, N'', N'', N'张步峰，男，91岁，头晕头痛反复发作二十余年入院。患者二十余年前无明显诱因下出现头晕头痛，在松江区中心医院，诊断为“高血压病”。给予硝苯地平片口服，平素监测血压可，具体数值不详。因年事已高，家中无人照顾入住本院。本次入院体检提示胆囊结石、尿酸偏高、肾囊肿。发病以来患者无胸闷心悸，无发热，无呕吐腹泻等，饮食、睡眠均可，大小便正常。
患者慢性过敏性皮炎目前服用西替利嗪片；患者因尿路感染目前服用左氧氟沙星片。体检：体温 36.4℃，脉搏70次/分，呼吸20次/分，血压140/86mmHg。全身皮肤可见散在皮疹，两肺呼吸音粗，心率70次/分，心律齐，腹部软，无压痛，四肢无水肿，无肌肉萎缩。糖化血红蛋白：5.8%，低密度脂蛋白：2.58mmol/L，甘油三酯：1.11mmol/L，高密度脂蛋白：1.08mmol/L，总胆固醇：3.99mmol/L，白蛋白：41.5g/L，谷丙转氨酶：12.86u/L，间接胆红素：6.02umol/L，碱性磷酸酶（ALP）：87.87u/L，总胆红素：10.83umol/L，总蛋白：71.07g/L。球蛋白：29.57g/L。直接胆红素：4.81umol/L。肌酐：92.07umol/L，尿素氮：7.12mmol/L，尿酸：420.22umol/L。心电图：窦性心律，房性早搏，异常Q波，ST改变。B超：肝内钙化灶，胆囊结石，左肾囊肿，胰腺、脾脏、右肾未见明显占位。CT：心脏增大，主动脉、冠状动脉硬化，双肺散在少量慢性炎症/纤维灶，慢性支气管炎改变，两侧放射冠、基底节区腔隙灶，老年脑，脑白质变性（2023.3.21上海市松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'543', N'1266', N'患者家属', N'可靠', N'袁纯兰', N'2023-03-30 00:00:00.000', NULL, NULL, N'', N'', N'蒋月勤，女，71岁，患者约两年前无明显诱因下出现头晕、口齿不清伴行走不稳，在松江区中心医院就诊，行CT检查提示脑萎缩，未予特殊治疗。因生活不能自理今入住我院。本次入院体检提示空腹血糖偏高，诊断为糖尿病，给予西格列汀二甲双胍和伏格列波糖口服。发病以来，患者无发热，无头痛呕吐，无咳嗽，无胸闷气急等，饮食睡眠尚可。体检：体温 36.5℃，脉搏84次/分，呼吸20次/分，血压132/85mmHg。两肺呼吸音清，心率84次/分，心律齐，腹软，无压痛，四肢无水肿，左上肢肌力为4级,左下肢肌力为4级,右上肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：17.27mmol/L。糖化血红蛋白：11.1%，低密度脂蛋白：3.02mmol/L，甘油三酯：0.76mmol/L，高密度脂蛋白：2.19mmol/L，总胆固醇：5.29mmol/L，白蛋白：46.63g/L，谷丙转氨酶：13.79u/L，间接胆红素：10.6umol/L，碱性磷酸酶（ALP）：113.12u/L，总胆红素：16.47umol/L，总蛋白：69.85g/L。球蛋白：23.22g/L。直接胆红素：5.87umol/L。肌酐：36.11umol/L，尿素氮：4.36mmol/L，尿酸：138.09umol/L。心电图：窦性心律，T波改变。B超：脂肪肝，肝囊肿，胆囊增大，胆囊结石胆泥淤积，胆总管稍扩张，双肾囊肿，胰腺、脾脏未见明显占位。CT：两肺上叶结节，主动脉硬化，肝左叶囊性灶，右肾低密度灶，胆囊结石。老年脑，脑白质变性（2023.3.24松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'546', N'1269', N'患者家属', N'可靠', N'袁纯兰', N'2023-04-18 00:00:00.000', NULL, NULL, N'', N'', N'周石泉，男，78岁，记忆力减退三年余，加重伴伤人行为三月余。患者于3年余年前开始出现记忆力减退，易忘事，不记得家庭住址，不记得已吃过饭，一直诉说有人偷家里的东西，看到有好多人在他周围，在上海市精神卫生中心就诊，诊断“阿尔茨海默病伴精神障碍”，具体用药不详。后一直在松江区精神卫生中心就诊并配备富马酸喹硫平片和盐酸多奈哌齐口服，因近3个月以来患者出现无故伤人行为给予加服奥氮平片。因患者年事已高，生活不能自理，家属无力照顾于2023年4月18日入住我院。发病以来无明显消瘦，无发热，无呕吐腹泻，饮食可，夜眠尚可，大小便正常。体检：体温 36.9℃，脉搏80次/分，呼吸20次/分，血压136/87mmHg。两肺呼吸音粗，心率80次/分，心律齐，腹软，无压痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。生化:葡萄糖：5.18mmol/L。Rbc：4.33×10<sup>12</sup>/L，Hb：141g/L，Wbc：4.82×10<sup>9</sup>/L，中性粒比例：71.5%，淋巴细胞比例：19.5%，Plt：198×10<sup>9</sup>/L。糖化血红蛋白：5.5%，低密度脂蛋白：2.88mmol/L，甘油三酯：1.76mmol/L，高密度脂蛋白：0.72mmol/L，总胆固醇：4.18mmol/L（2023.4.7上海市第一人民医院）。Cl：98.2mmol/L，K：3.46mmol/L，Na：139mmol/L，A/G：1.86，白蛋白：44.2g/L，谷丙转氨酶：27.7u/L，间接胆红素：11.8umol/L，碱性磷酸酶（ALP）：98u/L，总胆红素：18.8umol/L，总蛋白：68g/L。球蛋白：23.8g/L。直接胆红素：7umol/L。肌酐：90.2umol/L，尿素氮：3.82mmol/L，尿酸：394.1umol/L，心电图：窦性心律。B超：肝囊肿、前列腺增生。CT：肺气肿，左肺上叶条索灶，右肺钙化点，纵膈稍左移，心脏大，胸主动脉及冠状动脉硬化，甲状腺增大，肝囊肿，双侧额顶部硬膜下少量积液，双侧基底节区腔隙灶，脑萎缩，脑白质病，颅内动脉硬化（2023.4.7上海市第一人民医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'548', N'1271', N'患者本人及家属', N'可信', N'庄秋丽', N'2023-04-21 00:00:00.000', NULL, NULL, NULL, NULL, N'丁爱凤，女，78岁，两下肢功能障碍4年左右。  患者自2002年至2023年先后发生3次脑梗死，2002年第一次脑梗死后基本生活均能自理，据说大概自2019年起两下肢行走困难，后长期卧床，2020年10月再次发生口角歪斜，于上海闵行区中心医院就诊头颅CT：脑梗死，第三次脑梗死发生于2022年9月10日，于上海闵行区中心医院头颅CT：双侧基底节区多发腔隙性脑梗，缺血灶。目前长期卧床，基本生活不能自理，两下肢功能障碍。
  患者原有2型糖尿病史20余年，平时常注射胰岛素控制血糖，据说血糖控制可。
  目前一般情况可，无发热，纳食可，两便无异常。
  患者原有糖尿病史20余年，脑梗死20余年，否认曾有高血压病、心脏病、慢性支气管炎等其他慢性病史。
  体检：体温 36.4℃，脉搏84次/分，呼吸18次/分，血压122/77mmHg。神清，巩膜清，唇不绀，两肺呼吸音清，未及明显干湿啰音，84次/分，心律齐，腹软，全腹无压痛肝脾肋下未及肿大，两下肢膝关节以下水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级，左下肢肌力为2级，右上肢肌力为5级，右下肢肌力为2级。生化:葡萄糖：7.4mmol/L。Rbc：3.97×10<sup>12</sup>/L，Hb：123g/L，Wbc：9.76×10<sup>9</sup>/L，中性粒比例：61.9%，Plt：318×10<sup>9</sup>/L。糖化血红蛋白：6.20%，低密度脂蛋白：2.44mmol/L，甘油三酯：2.37mmol/L，高密度脂蛋白：1.30mmol/L，总胆固醇：4.93mmol/L。Cl：100mmol/L，K：4.8mmol/L，Na：139mmol/L，A/G：1.2，白蛋白：44g/L，碱性磷酸酶（ALP）：59u/L，总胆红素：6.0umol/L，总蛋白：81g/L。球蛋白：37g/L。直接胆红素：3.6umol/L。肌酐：68umol/L，尿素氮：10.1mmol/L，尿酸：366umol/L，心电图：窦性心动过速，逆钟向转位，T波改变。B超：脂肪肝，胆囊结石，右肾囊性病灶。CT：左肺下叶及右肺上叶实性结节，倾向于良性。纵膈多发淋巴结增大。胆囊多发结石。左肾多发微小结石，右肾囊肿。双侧基底节区及左侧半卵园中心软化灶形成，双侧基底节区及左侧半卵园区多发腔隙性脑梗死、缺血灶，老年脑改变。
')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'551', N'1274', N'患者本人及家属', N'可信', N'庄秋丽', N'2023-04-27 00:00:00.000', NULL, NULL, NULL, NULL, N'柳龙英，女，83岁，反复胸闷不适4年左右，患者7-8年前时常感觉头晕不适，自诉于医院检查提示“早搏”，于2019年开始出现反复胸闷，时出冷汗，动则气促，于医院心电图检查：“心动过缓”，同年予以植入心脏起搏器，安装后自诉仍时有反复胸闷不适，出冷汗现象，但自诉症状比之前有所好转，稍作活动仍有气促感，无胸痛心悸等现象。
本次入院前体检（2023.4.12松江中心医院）CT：双侧基底节区腔隙灶，老年脑改变。
患者目前一般情况可，无发热，纳食可，偶咳无痰，两便无异常，夜间睡眠差。
否认曾有高血压病，糖尿病，慢性支气管炎等其他慢性疾病。
体检：体温 36.6℃，脉搏60次/分，呼吸20次/分，血压123/79mmHg。神清，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，未及明显干湿啰音，心率60次/分，心律齐，腹软，全腹无压痛，无反跳痛，肝脾肋下未及肿大，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫，左上肢肌力为5级，左下肢肌力为5级，右上肢肌力为5级，右下肢肌力为5级。
生化:葡萄糖：5.85mmol/L。Rbc：3.74×10<sup>12</sup>/L，Hb：123g/L，Wbc：5.92×10<sup>9</sup>/L，中性粒比例：69.70%，淋巴细胞比例：22.30%，Plt：186×10<sup>9</sup>/L。糖化血红蛋白：5.30%，低密度脂蛋白：2.10mmol/L，甘油三酯：1.32mmol/L，高密度脂蛋白：1.27mmol/L，总胆固醇：3.66mmol/L。Ca：2.25mmol/L，Cl：103.62mmol/L，K：4.67mmol/L，Na：140.64mmol/L，P：1.34mmol/L。A/G：1.93，白蛋白：45.92g/L，谷丙转氨酶：13.03u/L，间接胆红素：5.72umol/L，碱性磷酸酶（ALP）：64.14u/L，总胆红素：9.66umol/L，总蛋白：69.74g/L。球蛋白：23.82g/L。直接胆红素：3.94umol/L。肌酐：114.52umol/L，尿素氮：8.98mmol/L，尿酸：356.29umol/L，白细胞：3.00u/L，管型：0u/L，尿胆原：阴性，葡萄糖：阴性mmol/L，酸碱度：6.0，酮体：阴性，上皮细胞：0u/L，亚硝酸盐：阴性，隐血：+。心电图：窦性心律，ST II III aVF  V4-V6水平压低0.05-0.10mv
 T II III aVF低平倒置。B超：肝脏囊肿，胆囊术后未显示，肝内外胆管未见明显扩张，胰腺、脾脏、双肾未见明显占位。CT：心脏增大，心脏起搏器植入中，主动脉硬化，右肺上叶实性小结节。双侧基底节区腔隙灶，老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'563', N'1286', N'患者家属', N'基本可靠', N'胡新志', N'2023-07-07 00:00:00.000', NULL, NULL, N'', N'', N'黄辉，男，53岁，多饮多食五年余。患者五年前无诱因出现多饮多食，体型消瘦，在虹口中心医院就诊，诊断：“2型糖尿病”长期口服阿卡波糖胶囊，血糖控制尚稳定维持在6.0-9.0mmol/l之间。
患者3岁时坠楼，致颅脑外伤、右侧肢体畸形功能障碍、认知障碍，长期坐轮椅。癫痫史40年，长期口服苯妥英钠片、苯巴比妥片。
入院胸部CT体检提示：肺炎。尿常规：泌尿系感染。
现因行动不便，个人生活不能自理，于20230707自愿入住我院。患者一般情况尚可，轮椅推入病房，口齿不清，简单对答，查体合作，饮食可、睡眠尚可，大小便正常。否认肝炎、结核传染病史体检：体温 36.9℃，脉搏76次/分，呼吸18次/分，血压117/74mmHgmmHg。两肺呼吸音清，80次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为3级,右下肢肌力为4级。右手畸形，右足内翻。生化:葡萄糖：4.81mmol/L。糖化血红蛋白：5.00%（2023-7-7松江中心医院），低密度脂蛋白：4.03mmol/L（2023-7-7松江中心医院），甘油三酯：0.57mmol/L，高密度脂蛋白：1.86mmol/L，总胆固醇：5.96mmol/L。Ca：2.23mmol/L，Cl：98.67mmol/L，K：3.67mmol/L，Na：137.62mmol/L，P：1.09mmol/L（2023-7-7松江中心医院）。A/G：1.37，白蛋白：42.04g/L，谷丙转氨酶：15.85u/L，间接胆红素：1.74umol/L，碱性磷酸酶（ALP）：110.49u/L（2023-7-7松江中心医院），总胆红素：3.35umol/L，总蛋白：72.76g/L。球蛋白：30.72g/L。直接胆红素：1.61umol/L。肌酐：55.19umol/L，尿素氮：4.78mmol/L，尿酸：263.19umol/L，白细胞：阴性u/L，管型：阴性u/L，尿胆原：+，葡萄糖：阴性mmol/L，酸碱度：阴性，酮体：阴性，上皮细胞：阴性u/L（2023-7-7松江中心医院），亚硝酸盐：阴性，隐血：+。心电图：正常心电图（2023-7-7松江中心医院）X线胸片：(缺)。B超：肝脏囊肿、肝内实性结节，胆囊、胰腺脾脏显示区、双肾未见明显占位（2023-7-7松江中心医院）CT：头颅CT：1.左侧颞顶枕叶软化灶，左侧颞顶骨缺如2.四叠体池左侧脂肪瘤。胸部CT:1.右肺中叶及左肺下叶少许炎性改变2.两肺下叶纤维灶3.附见：肝内散在囊性灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'571', N'1294', N'患者家属', N'基本可靠', N'胡新志', N'2023-09-11 00:00:00.000', NULL, NULL, N'', N'', N'金祥英，女，71岁，反复头晕头痛20余年。患者二十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服替米沙坦片片，血压控制稳定，维持在110-150/70-90之间。入院体检提示：慢性支气管炎、肺大泡，肺结节，脂肪肝、肝脏多发囊肿，双肾囊肿、左肾小结石。
患者一般情况尚可，轮椅推入病房，反应迟钝，听力减退，查体合作，饮食可，睡眠尚可，大便正常，小便频繁。否认肝炎、结核等传染病史。否认糖尿病史。体检：体温 36.7℃，脉搏74次/分，呼吸18次/分，血压133/87mmHg，两肺呼吸音粗，心率74次/分，律齐，无腹壁紧张，无压痛，四肢无水肿，,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为3级。右脚踝骨折史二十余年。生化:葡萄糖：5.33mmol/L，糖化血红蛋白：5.9%低密度脂蛋白：1.41mmol/L，甘油三酯：0.96mmol/L，高密度脂蛋白：2.22mmol/L，总胆固醇：3.68mmol/L。Ca：2.31mmol/L，Cl：105.88mmol/L，K：5.03mmol/L，Na：142.57mmol/L，P：1.25mmol/L。A/G：1.91，白蛋白：43.30g/L，谷丙转氨酶：19.71u/L，间接胆红素：6.31umol/L，碱性磷酸酶（ALP）：102.55u/L，总胆红素：12.27umol/L，总蛋白：65.98g/L。球蛋白：22.68g/L。直接胆红素：5.96umol/L。肌酐：66.01umol/L，尿素氮：10.14mmol/L，尿酸：226.97umol/L，白细胞：21u/L，尿胆原：阴性，葡萄糖：阴性mmol/L，酮体：阴性，亚硝酸盐：阴性，隐血：阴性。心电图：正常心电图，B超：脂肪肝，胆囊、胰腺、脾脏、双肾未见明显占位，CT：胸部CT：1.左肺上叶、右肺中叶小结节2.右肺下叶小钙化灶 附见甲状腺右叶增大伴斑片状低密度影及点状钙化灶。头颅CT：双侧放射冠区腔隙灶，老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'607', N'1330', N'患者本人及家属', N'基本可信', N'庄秋丽', N'2024-05-20 00:00:00.000', NULL, NULL, NULL, NULL, N'唐爱金，女，85岁，患者记忆力差，认知能力下降2月余。患者2024年3月7日上厕所时感两下肢无力跌倒，伴有口齿不清，于松江中心医院就诊，经检查诊断：大脑动脉血栓引起脑梗死，2024年3月10-3月18日入住于松江中心医院治疗，病情稳定出院，目前渐进性出现记忆力下降，认知能力下降，时糊涂，乱拆损坏东西。
患者原有原发性高血压病10余年，平时无头头痛等不适，最高血压180/90mmHg，常服药控制血压，据说血压控制可。
患者本次脑梗死跌倒引起右侧髌骨损伤，之后时常出现膝关节酸痛，右关节稍僵硬，屈曲稍受限，行走稍坡，目前酸痛缓解。
患者2024年3月10-3月18日入住于松江中心医院治疗期间发现有糖尿病，目前服用降血糖药控制血糖，血糖控制可。
患者2024年3月18日松江中心医院出院诊断提示有胆囊结石胆囊炎，低钾血症。
患者目前一般情况可，无发热，纳食可，两便无异常，夜间睡眠可。
体检：体温 36.6℃，脉搏70次/分，呼吸20次/分，血压130/75mmHg。神志清，精神可，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率70次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，右下肢膝关节稍僵硬，屈曲稍受限，行走稍坡，右膝无红肿压痛，其余三肢体活动可。生化:葡萄糖：7.37mmol/L。Rbc：4.43×10<sup>12</sup>/L，Hb：137g/L，Wbc：3.88×10<sup>9</sup>/L，中性粒比例：56%，淋巴细胞比例：29.10%，Plt：228×10<sup>9</sup>/L。C反应蛋白：11.11MG/L，糖化血红蛋白：6.5%，低密度脂蛋白：2.53mmol/L，甘油三酯：1.92mmol/L，高密度脂蛋白：1.11mmol/L，总胆固醇：4.02mmol/L。Ca：2.24mmol/L，Cl：97.37mmol/L，K：3.22mmol/L，Na：137.89mmol/L，P：0.92mmol/L。A/G：1.39，白蛋白：38.59g/L，谷丙转氨酶：6.82u/L，间接胆红素：12.52umol/L，碱性磷酸酶（ALP）：91.65u/L，总胆红素：19.62umol/L，总蛋白：66.28g/L。直接胆红素：7.10umol/L。肌酐：67.45umol/L，尿素氮：4.58mmol/L，尿酸：372.76umol/L，白细胞：47u/L，管型：1.00u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：7.0，酮体：阴性（-），上皮细胞：63u/L，亚硝酸盐：阴性（-），隐血：阴性（-）。心电图：窦性心律，V1R/S>1，T波改变，V4-V6低平。X线胸片：(缺)。B超：脂肪肝，肝脏囊肿，肝内钙化灶，胆囊炎、胆囊结石。CT：老年脑改变，两侧基底节区及半卵圆中心多发腔隙灶，部分软化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'619', N'1342', N'患者家属', N'可信', N'王屹', N'2024-06-28 00:00:00.000', NULL, NULL, NULL, NULL, N'王纪岳，男，85岁，反复头晕不适十余年。患者十余年前无明显诱因下出现头晕头痛，经松江区中心医院诊治，诊断为：高血压，予氨氯地平片等控制血压，一年前患者出现血压偏低，暂停服药。患者一个月前因反复咳嗽咳痰伴乏力纳差，住院治疗，诊断为：社区获得性肺炎，予抗菌祛痰纠正电解质等对症治疗，咳嗽咳痰症状较前缓解，今出院入住本院，继续予头孢地尼分散片、盐酸氨溴索胶囊、复方鲜竹沥液等治疗（外配）。患者住院期间经多科室会诊，诊断为：帕金森综合征、脑梗死后遗症、会厌溃疡、抗肾小球基底膜抗体病、低蛋白血症等，目前予多巴丝肼片、奥美拉唑肠溶胶囊等治疗。患者原有前列腺增生史数年予盐酸坦索罗辛缓释胶囊、非那雄胺片治疗；睡眠障碍史二年余予艾司唑仑片治疗；便秘史一年余予乳果糖口服溶液、开塞露等治疗。患者目前一般情况较差，明显消瘦，纳差，进食呛咳明显，生活自理能力减退。患者五年前因房颤行心脏射频消融术。体检：体温 36.2℃，脉搏77次/分，呼吸18次/分，血压112/61mmHg。两肺呼吸音粗，心率77次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，肌张力增强。生化:葡萄糖：4
mmol/L（松江区中心医院2024.6.25）。Rbc：4.64×10<sup>12</sup>/L，Hb：134g/L，Wbc：6.09×10<sup>9</sup>/L，中性粒比例：4.37%，淋巴细胞比例：1.21%，Plt：228
×10<sup>9</sup>/L（松江区中心医院2024.6.25）。C反应蛋白：37.91
MG/L（松江区中心医院2024.6.25）K：3.92mmol/L，Na：130mmol/L，白蛋白：31.4
g/L（松江区中心医院2024.6.13），谷丙转氨酶：5.7u/L，肌酐：65umol/L，尿素氮：6.3mmol/L，尿酸：223
umol/L（松江区中心医院2024.6.14），心电图：窦性心律、房性早搏、T波改变
（（松江区中心医院2024.6.15）CT：两肺炎症，主动脉、冠状动脉硬化，两侧胸腔少量积液
（松江区中心医院2024.6.13）
双侧基底节及双侧发射区冠腔隙灶、老年脑改变
（松江区中心医院2024.6.24）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'345', N'1061', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'346', N'1063', N'其他', N'基本可靠', N'周婷', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'365', N'1087', N'患者家属', N'基本可靠', N'张丁', N'2019-04-19 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'366', N'1088', N'患者家属', N'基本可靠', N'张丁', N'2019-04-22 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'372', N'1094', N'患者家属', N'基本可靠', N'胡新志', N'2019-09-19 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'379', N'1100', N'其他', N'可靠', N'袁纯兰', N'2020-05-11 00:00:00.000', NULL, NULL, N'二福院工作人员', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'397', N'1119', N'患者家属', N'基本可靠', N'胡新志', N'2021-04-22 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'400', N'1121', N'患者家属', N'可信', N'袁纯兰', N'2021-04-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'401', N'1124', N'患者家属', N'基本可靠', N'胡新志', N'2021-04-27 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'403', N'1126', N'患者本人及家属', N'可信', N'袁纯兰', N'2021-05-11 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'412', N'1135', N'患者本人及家属', N'可信', N'袁纯兰', N'2021-11-08 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'434', N'1158', N'患者家属', N'基本可信', N'庄秋丽', N'2022-09-01 00:00:00.000', NULL, NULL, NULL, NULL, N'曹宝娟，女，82岁，左侧肢体功能障碍17余年。患者17年前突然中风，于松江区中心医院就诊头颅CT检查结果“脑溢血”，后住院治疗病情稳定，但留下左侧肢体偏瘫后遗症。7-8年前发生“脑梗死”，于松江中心医院CT确诊。患者目前左上肢举手握拳无力，不能独自站立行走，基本生活不能自理。
患者原有原发性高血压病史40-50年，最高血压180/90mmHg，2年前规律服药，据说血压控制可，但现已停药2年，据说目前常测血压正常。
患者近2年夜间睡眠差，常服用艾司唑仑片利眠药入睡
患者此次入院前检查心电图：ST—T改变，ST I II V。4-V6水平压低0.05-0.10mv，T I avL  V4-V6低平或浅倒置。B超：脂肪肝，胆囊炎、胆囊结石及胆泥。CT：心脏增大，主动脉及冠状动脉硬化，少量心包积液。右侧丘脑及右侧基底节区多发腔隙灶。老年脑，脑白质变性。
目前一般情况可，无发热，纳食可，两便无异常。
患者原有原发性高血压病40-50年，脑溢血史17年，脑梗死7-8年。
体检：体温 36.7℃，脉搏68次/分，呼吸20次/分，血压193/86mmHg。神清，巩膜清，结膜无异常，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，未闻及干湿啰音，心律68次/分，心律齐，腹软，全腹无压痛，肝脾肋下未及肿大，四肢无水肿，左侧肢体偏瘫，上肢肌力为2级，左下肢肌力为2级，右上肢肌力为5级，右下肢肌力为5级。
生化:葡萄糖：5.38mmol/L。Rbc：4.57×10<sup>12</sup>/L，Hb：136g/L，Wbc：6.37×10<sup>9</sup>/L，中性粒比例：63%，淋巴细胞比例：27.60%，Plt：246×10<sup>9</sup>/L。糖化血红蛋白：5.50%，低密度脂蛋白：1.70mmol/L，甘油三酯：1.35mmol/L，高密度脂蛋白：0.84mmol/L，总胆固醇：2.88mmol/L。Ca：2.14mmol/L，Cl：103.19mmol/L，K：3.44mmol/L，Na：142.72mmol/L，P：1.10mmol/L。A/G：1.20，白蛋白：39.51g/L，谷丙转氨酶：15.79u/L，间接胆红素：7.90umol/L，碱性磷酸酶（ALP）：114.63u/L，总胆红素：14.01umol/L，总蛋白：72.31g/L。球蛋白：32.80g/L。直接胆红素：6.11umol/L。肌酐：55.62umol/L，尿素氮：3.01mmol/L，尿酸：243.84umol/L，心电图：窦性心律，ST—T改变，ST I II V4-V6水平压低0.05-0.10mv，T I avL  V4-V6低平或浅倒置。B超：脂肪肝，胆囊炎、胆囊结石及胆泥，双肾囊肿，胰腺、脾脏未见明显占位。CT：心脏增大，主动脉及冠状动脉硬化，少量心包积液。右侧丘脑及右侧基底节区多发腔隙灶。老年脑，脑白质变性。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'438', N'1163', N'患者本人及家属', N'可信', N'袁纯兰', N'2022-11-01 00:00:00.000', NULL, NULL, NULL, NULL, N'李菊稚，男，71岁，自幼愚笨，生活能力差71年。患者智力低下七十一年。平素能与人简单交流，但缺乏思维能力，有时答非所问。能自行吃饭、穿脱衣等。因年事渐高，生活不能完全自理于2022年11月1日入住本院。目前一般情况可，纳可，大小便正常，夜眠可。 
患者既往前列腺增生病史，目前口服非那雄胺片和坦索罗辛缓释胶囊。
湿疹皮炎病史3年，前列腺增生病史10余年体检：体温 36.5℃，脉搏66次/分，呼吸20次/分，血压147/86mmHg。两肺呼吸音清，无湿啰音。65次/分，心律齐，腹软，无压痛，无反跳痛。四肢无水肿。生化:葡萄糖：4.73mmol/L（2022.8.19松江区中心医院）。Rbc：4.54×10<sup>12</sup>/L，Hb：146g/L，Wbc：6.3×10<sup>9</sup>/L，中性粒比例：72.4%，淋巴细胞比例：17.1%，Plt：199×10<sup>9</sup>/L（2022.8.19松江区中心医院）。糖化血红蛋白：5.3%（2022.8.19松江区中心医院），低密度脂蛋白：2.87mmol/L（2022.8.19松江区中心医院），甘油三酯：0.93mmol/L，高密度脂蛋白：1.24mmol/L，总胆固醇：4.37mmol/L。Ca：2.13mmol/L，Cl：102.83mmol/L，K：4.88mmol/L，Na：138.18mmol/L，P：0.91mmol/L（2022.8.19松江区中心医院）。A/G：1.69，白蛋白：45.61g/L，谷丙转氨酶：14.29u/L，间接胆红素：7.48umol/L，碱性磷酸酶（ALP）：87.34u/L（2022.8.19松江区中心医院），总胆红素：11.42umol/L，总蛋白：72.55g/L。球蛋白：26.94g/L。直接胆红素：3.94umol/L。肌酐：87.07umol/L，尿素氮：4.07mmol/L，尿酸：282.12umol/L（2022.8.19松江区中心医院）。心电图：正常心电图（2022.8.19松江区中心医院）B超：肝脏多发囊肿，胆囊、胰腺、脾脏、双肾未见明显占位（2022.8.19松江区中心医院）CT：老年脑改变（2022.8.19松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'439', N'1164', N'患者家属', N'基本可靠', N'胡新志', N'2022-11-08 00:00:00.000', NULL, NULL, N'', N'', N'徐明德，男，79岁，反复头晕头痛二十五年余。患者二十五年前无诱因反复出现头晕头痛不适，无恶心、呕吐，无视物旋转，无黑朦，无胸闷心悸不适，在松江中心医院就诊，诊断“高血压病”，长期口服氨氯地平片等药物，血压维持在110-150/70-90mmHg之间，血压控制较稳定。
二十余年前体检发现“糖尿病”，长期服用盐酸二甲双胍缓释片、瑞格列奈片、德谷胰岛素治疗，血糖控制不佳，导致血管周围神经病，双足内翻畸形，双下肢行走困难，个人生活不能自理。于20221108自愿入住我院。体检：体温 36.5℃，脉搏63次/分，呼吸20次/分，血压124/72mmHg。两肺呼吸音清，心率63次/分，律不齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为3级。双下肢皮肤可见散在皮疹及抓痕，大腿内侧尤甚。双足内翻畸形。生化:葡萄糖：8.33mmol/L（2022-10-17上海市第五康复医院）。Rbc：4.65×10<sup>12</sup>/L，Hb：138g/L，Wbc：11.94×10<sup>9</sup>/L，中性粒比例：84.2%，淋巴细胞比例：70.6%，Plt：248×10<sup>9</sup>/L（2022-10-17上海市第五康复医院）。糖化血红蛋白：8.1%，（2022-10-17上海市第五康复医院），甘油三酯：2.57mmol/L，总胆固醇：2.4mmol/L。Ca：2.42mmol/L（2022-10-17上海市第五康复医院），Cl：103.9mmol/L，K：3.92mmol/L，Na：139.8mmol/L，A/G：1.21，白蛋白：44.4g/L，谷丙转氨酶：11.3u/L，间接胆红素：4.56umol/L，碱性磷酸酶（ALP）：79.5u/L（2022-10-17上海市第五康复医院），总胆红素：7.9umol/L，总蛋白：81.06g/L。球蛋白：36.66g/L。直接胆红素：3.34umol/L。肌酐：86.9umol/L，尿素氮：6.84mmol/L，尿酸：370umol/L（2022-10-17上海市第五康复医院），心电图：窦性心律 室性早搏左室 高电压 I度房室传导阻滞 ST段改变（2022-10-17上海市第五康复医院）X线胸片：(缺)。B超：前列腺增生伴钙化 肝脏 胰腺 脾脏 双肾 膀胱未见明显异常 两侧输尿管未见明显异常（2022-10-17上海市第五康复医院）CT：胸部CT+头颅CT提示：1.左侧丘脑区腔隙性梗塞；老年脑改变2.左肺下叶轻度炎症3.主动脉及冠状动脉硬化（2022-10-17上海市第五康复医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'441', N'1186', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦丽，女，67岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似聋哑，日常生活中洗漱、进食、穿脱衣服、行走、如厕等可以独立完成，洗浴需要辅助。本次入院体检提示窦性心动过缓，血脂偏高。肺结核病史数年，目前无治疗。体检：体温 36.4℃，脉搏74次/分，呼吸20次/分，血压122/80mmHg。两肺呼吸音粗，无干啰音、湿啰音、心率74次/分，心律齐，未闻及早搏腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.04mmol/L（2022.10.10上海市上农医院）。Rbc：4.63×10<sup>12</sup>/L，Hb：137g/L，Wbc：6.75×10<sup>9</sup>/L，中性粒比例：52.7%，淋巴细胞比例：31.7%，Plt：241×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：3.41mmol/L，甘油三酯：1.33mmol/L，高密度脂蛋白：1.58mmol/L，总胆固醇：5.76mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：19u/L，碱性磷酸酶（ALP）：64u/L，总胆红素：6.4umol/L（2022.10.10上海市上农医院），肌酐：68umol/L，尿素氮：6.15mmol/L，尿酸：353umol/L（2022.10.10上海市上农医院），心电图：窦性心律，窦性心动过缓（2022.10.10上海市上农医院）X线胸片：（-）B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'442', N'1170', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'陈新财，男，67岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、洗浴、行走、如厕等方面能够独立完成。入院前体检提示窦性心动过缓，肝硬化，胆囊未显示，肝功能异常，血小板偏低，HBsAg阳性，HBcAb阳性。体检：体温 36.1℃，脉搏98次/分，呼吸19次/分，血压101/71mmHg。两肺呼吸音粗，无干湿啰音。心率98次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.44mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：4.89×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：156g/L，Wbc：5.37×10<sup>9</sup>/L，中性粒比例：72.1%，淋巴细胞比例：21.6%，Plt：91×10<sup>9</sup>/L。低密度脂蛋白：2.31mmol/L，甘油三酯：0.58mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：1.59mmol/L，总胆固醇：4.33mmol/L。谷丙转氨酶：146u/L（南京艾迪康医学检验所，2022-10-10），碱性磷酸酶（ALP）：199u/L，总胆红素：18.1umol/L，肌酐：82umol/L，尿素氮：6.23mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：243umol/L，心电图：窦性心动过缓。（上海市上农医院，2022-10-10）X线胸片：（-）（上海市上农医院，2022-10-10）B超：肝硬化，胆囊未显示。（上海市上农医院，2022-10-10）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'450', N'1188', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦梅，女，62岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似聋哑，日常生活中洗漱、进食、穿脱衣服、行走、如厕等可以独立完成，洗浴需要辅助。本次入院体检提示窦性心动过缓，高脂血症、乙型病毒性肝炎。体检：体温 36.9℃，脉搏67次/分，呼吸20次/分，血压98/77mmHg。两肺呼吸音粗，无干啰音湿啰音，心率67次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.31mmol/L。Rbc：4.31×10<sup>12</sup>/L，Hb：130g/L，Wbc：5.19×10<sup>9</sup>/L，中性粒比例：56.5%，淋巴细胞比例：36.5%，Plt：165×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：3.06mmol/L，甘油三酯：0.84mmol/L，高密度脂蛋白：2.29mmol/L，总胆固醇：6.02mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：12u/L，碱性磷酸酶（ALP）：91u/L，总胆红素：6.5umol/L（2022.10.10上海市上农医院），肌酐：60umol/L，尿素氮：7.62mmol/L，尿酸：228umol/L（2022.10.10上海市上农医院），心电图：窦性心动过缓X线胸片：心影增大B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'452', N'1190', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦淑，女，74岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、行走、如厕等可以独立完成，洗浴、穿脱衣服需要辅助。本次入院体检提示高血压病、I度房室传导阻滞。 体检：体温 36.2℃，脉搏78次/分，呼吸20次/分，血压142/88mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率78次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.93mmol/L（2022.10.10上海市上农医院）。Rbc：4.21×10<sup>12</sup>/L，Hb：137g/L，Wbc：4.81×10<sup>9</sup>/L，中性粒比例：52.5%，淋巴细胞比例：38.6%，Plt：194×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.98mmol/L，甘油三酯：0.62mmol/L，高密度脂蛋白：1.29mmol/L，总胆固醇：4.77mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：14u/L，碱性磷酸酶（ALP）：80u/L，总胆红素：6.7umol/L（2022.10.10上海市上农医院），肌酐：59umol/L，尿素氮：6.28mmol/L，尿酸：170umol/L（2022.10.10上海市上农医院），心电图：窦性心律，I°房室传导阻滞。（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'456', N'1184', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦静，女，62岁，智力低下数十年。  该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中进食、行走可以独立完成，穿脱衣服、洗漱、洗浴、如厕等需要辅助。日常生活中有撕衣服、抠脸等行为。患者目前服用阿托伐他汀钙片、二甲双胍吡嗪片、阿卡波糖片、苯磺酸氨氯地平片、吲达帕胺片等。本次入院体检提示高血压病、糖尿病、高脂血症、脂肪肝、高尿酸血症、胆囊结石。体检：体温 36.5℃，脉搏99次/分，呼吸20次/分，血压138/84mmHg。两肺呼吸音清，无干啰音湿啰音，心率99次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：10.78mmol/L（2022.10.10上海市上农医院）。Rbc：4.58×10<sup>12</sup>/L，Hb：136g/L，Wbc：9.5×10<sup>9</sup>/L，中性粒比例：50.6%，淋巴细胞比例：43.3%，Plt：361×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：3.89mmol/L，甘油三酯：3.71mmol/L，高密度脂蛋白：1.28mmol/L，总胆固醇：6.11mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：13u/L，碱性磷酸酶（ALP）：120u/L，总胆红素：7.5umol/L（2022.10.10上海市上农医院），肌酐：79umol/L，尿素氮：7.3mmol/L，尿酸：541umol/L（2022.10.10上海市上农医院），心电图：窦性心律，左室高电压（2022.10.10上海市上农医院）B超：脂肪肝、胆结石（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'349', N'1064', N'', N'基本可靠', N'张丁', N'2018-12-11 00:00:00.000', NULL, NULL, N'上海市第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'351', N'1069', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'352', N'1071', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'353', N'1072', N'其他', N'基本可靠', N'胡新志', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二社会福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'355', N'1079', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'363', N'1085', N'患者家属', N'基本可靠', N'胡新志', N'2019-03-05 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'387', N'1109', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-09-03 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'389', N'1111', N'患者本人及家属', N'基本可靠', N'胡新志', N'2020-09-14 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'399', N'1123', N'患者家属', N'基本可靠', N'胡新志', N'2021-04-26 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'404', N'1127', N'患者本人及家属', N'可信', N'袁纯兰', N'2021-05-20 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'407', N'1130', N'患者家属', N'基本可信', N'周婷', N'2021-10-12 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'410', N'1133', N'患者本人及家属', N'', N'袁纯兰', N'2021-10-22 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'411', N'1134', N'患者本人及家属', N'基本可信', N'周婷', N'2021-10-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'425', N'1149', N'患者本人及家属', N'可信', N'袁纯兰', N'2022-03-03 00:00:00.000', NULL, NULL, NULL, NULL, N'冯水珍，女，89岁，右下肢行走不利4年余。患者2017年8月不慎跌倒致右大腿外伤，于松江中心医院急诊检查为：右股骨骨折，给予内固定手术治疗。目前右下肢行走不利，平时借助助步器缓慢行走或者轮椅。2021年在康佳养老院不慎摔倒致左侧肋骨骨折和胸6椎体骨折，未作特殊治疗。因年事渐高，生活不能完全自理今入住我福利院。发病以来患者一般情况可，无发热，纳食可，两便无异常，夜眠可。否认肝炎、结核病史；患者既往慢性胃炎病史30余年，偶有腹痛反酸等不适给予奥美拉唑治疗后缓解；患者原有梅尼埃病30年余，发作后经休息后缓解，双眼白内障病史25年余，未作治疗。体检：体温 36.2℃，脉搏82次/分，呼吸20次/分，血压143/82mmHg。两肺呼吸音清，心率82次/分，心律齐，腹部软，无压痛，无反跳痛。四肢无水肿，右臀部肌肉萎缩，右下肢肌力为4级。中性粒比例：81.1%，淋巴细胞比例：14.2%，，低密度脂蛋白：3.3mmol/L，甘油三酯：1.48mmol/L，高密度脂蛋白：1.59mmol/L，总胆固醇：5.27mmol/L。碱性磷酸酶（ALP）：199.91u/L，心电图：正常心电图。CT：左肺下叶、右肺上叶实性小结节；两肺上叶小钙化灶，左肺上叶、右肺中叶纤维灶；主动脉、冠状动脉硬化。（2022.3.1松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'462', N'1182', N'其他', N'基本可靠', N'胡新志', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'夏泽美，女，63岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、穿脱衣服、洗浴需要辅助完成，进食、行走、如厕等可独立完成，本次入院体检提示高胆固醇血症、窦性心动过缓。否认肝炎、结核传染病史。体检：体温 36.7℃，脉搏73次/分，呼吸18次/分，血压95/64mmHg。两肺呼吸音清。心率73次/分，律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.56mmol/L（2022-10-10上海市上农医院）。Rbc：4.39×10<sup>12</sup>/L，Hb：134g/L，Wbc：4.68×10<sup>9</sup>/L，中性粒比例：70.3%，淋巴细胞比例：21.1%，Plt：150×10<sup>9</sup>/L（2022-10-10上海市上农医院）。低密度脂蛋白：4.19mmol/L（2022-10-10上海市上农医院），甘油三酯：1.02mmol/L，高密度脂蛋白：1.75mmol/L，总胆固醇：6.33mmol/L。谷丙转氨酶：13u/L，碱性磷酸酶（ALP）：42u/L（2022-10-10上海市上农医院），总胆红素：8.9umol/L，肌酐：58umol/L，尿素氮：7.00mmol/L，尿酸：241umol/L（2022-10-10上海市上农医院），心电图：窦性心动过缓（2022-10-10上海市上农医院）)。B超：（-）（2022-10-10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'463', N'1183', N'其他', N'基本可靠', N'胡新志', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦芳，女，73岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱无法完成，穿脱衣服、洗浴需要辅助完成，进食、行走、如厕可独立完成，本次入院体检提示高胆固醇血症、窦性心动过缓、肺气肿。否认肝炎、结核传染病史体检：体温 36.2℃，脉搏65次/分，呼吸18次/分，血压121/84mmHg。两肺呼吸音清，心率87次/分，律齐。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.34mmol/L（2022-10-10上海市上农医院）。Rbc：4.12×10<sup>12</sup>/L，Hb：131g/L，Wbc：5.32×10<sup>9</sup>/L，中性粒比例：64.1%，淋巴细胞比例：27.1%，Plt：131×10<sup>9</sup>/L（2022-10-10上海市上农医院）。低密度脂蛋白：3.02mmol/L（2022-10-10上海市上农医院），甘油三酯：0.55mmol/L，高密度脂蛋白：2.11mmol/L，总胆固醇：5.79mmol/L。谷丙转氨酶：10u/L，碱性磷酸酶（ALP）：78u/L（2022-10-10上海市上农医院），总胆红素：13.7umol/L，肌酐：74umol/L，尿素氮：8.95mmol/L，尿酸：275umol/L（2022-10-10上海市上农医院），心电图：窦性心动过缓（2022-10-10上海市上农医院）X线胸片：肺气肿（2022-10-10上海市上农医院）B超：胆囊未见（2022-10-10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'464', N'1172', N'其他', N'基本可靠', N'胡新志', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市', N'', N'董连珍，女，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣服无法完成，需要完全照顾，洗漱、进食、行走、如厕等可独立完成，本次入院体检提示胸腰椎段后凸畸形、窦性心律 T波改变。体检：体温 36.6℃，脉搏87次/分，呼吸18次/分，血压161/89mmHg。两肺呼吸音清。心率65次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.32mmol/L（2022-10-10上海市上农医院）。Rbc：3.87×10<sup>12</sup>/L，Hb：120g/L，Wbc：7.31×10<sup>9</sup>/L，中性粒比例：62.8%，淋巴细胞比例：28.9%，Plt：206×10<sup>9</sup>/L（2022-10-10上海市上农医院）。低密度脂蛋白：2.66mmol/L（2022-10-10上海市上农医院），甘油三酯：1.13mmol/L，高密度脂蛋白：1.40mmol/L，总胆固醇：4.60mmol/L。谷丙转氨酶：16u/L，碱性磷酸酶（ALP）：87u/L（2022-10-10上海市上农医院），总胆红素：10.6umol/L，肌酐：65umol/L，尿素氮：7.70mmol/L，尿酸：235umol/L（2022-10-10上海市上农医院），心电图：窦性心律 T波改变（2022-10-10上海市上农医院）X线胸片：（-）（2022-10-10上海市上农医院）B超：（-）（2022-10-10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'468', N'1179', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋柱子，男，64岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、行走、如厕等方面能够独立完成，洗浴方面需要辅助完成。入院前体检提示Ⅰ度房室传导阻滞，HBcAb(+)。体检：体温 36.0℃，脉搏92次/分，呼吸19次/分，血压149/88mmHg。两肺呼吸音粗，无干湿啰音。心率80次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.58mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：5.24×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：146g/L，Wbc：6.02×10<sup>9</sup>/L，中性粒比例：53.8%，淋巴细胞比例：35.5%，Plt：184×10<sup>9</sup>/L。低密度脂蛋白：3.23mmol/L，甘油三酯：1.15mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：1.24mmol/L，总胆固醇：4.70mmol/L。谷丙转氨酶：13u/L（南京艾迪康医学检验所，2022-10-10），碱性磷酸酶（ALP）：87u/L，总胆红素：8.1umol/L，肌酐：68umol/L，尿素氮：5.41mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：330umol/L，心电图：1、窦性心律；2、Ⅰ度房室传导阻滞窦性心律（上海市上农医院，2022-10-10）X线胸片：（-）（上海市上农医院，2022-10-10）B超：（-）（上海市上农医院，2022-10-10）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'470', N'1180', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'王素娟，女，76岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、行走、如厕等方面能够独立完成，穿脱衣、洗浴方面需要辅助完成。入院前体检提示TP-Ab(+)，TRust(-)，Ⅰ度房室传导阻滞，右室传导延迟。有梅毒史。体检：体温 36.7℃，脉搏64次/分，呼吸16次/分，血压118/80mmHg。两肺呼吸音粗，无干湿啰音。心率80次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.75mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：4.31×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：118g/L，Wbc：5.36×10<sup>9</sup>/L，中性粒比例：64.9%，淋巴细胞比例：29.0%，Plt：201×10<sup>9</sup>/L。低密度脂蛋白：2.92mmol/L，甘油三酯：0.50mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：1.44mmol/L，总胆固醇：4.75mmol/L。谷丙转氨酶：12u/L，碱性磷酸酶（ALP）：50u/L，总胆红素：15.5umol/L，肌酐：74umol/L，尿素氮：10.3mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：234umol/L，心电图：窦性心律，Ⅰ度房室传导阻滞，右室传导延迟（上海市上农医院，2022-10-10）X线胸片：正常（上海市上农医院，2022-10-10）B超：正常（上海市上农医院，2022-10-10）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'471', N'1258', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'陈世合，男，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。入院前体检提示T波改变，HBsAb阳性。体检：体温36.8℃，脉搏87次/分，呼吸20次/分，血压129/94mmHg。两肺呼吸音清，无干湿啰音。心率87次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。左脚大趾小趾缺损。生化:葡萄糖：4.92mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：5.08×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-12），Hb：156g/L，Wbc：6.48×10<sup>9</sup>/L，中性粒比例：64.8%，淋巴细胞比例：28.0%，Plt：178×10<sup>9</sup>/L。低密度脂蛋白：3.25mmol/L，甘油三酯：0.98mmol/L（南京艾迪康医学检验所，2022-10-12），高密度脂蛋白：1.46mmol/L，总胆固醇：5.10mmol/L。谷丙转氨酶：17u/L（南京艾迪康医学检验所，2022-10-12），碱性磷酸酶（ALP）：94u/L，总胆红素：11.1umol/L，肌酐：72umol/L，尿素氮：5.23mmol/L，尿酸：271umol/L，心电图：T波改变。（上海市上农医院，2022-10-12）X线胸片：正常。（上海市上农医院，2022-10-12）B超：正常。（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'359', N'1077', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'361', N'1075', N'其他', N'可靠', N'袁纯兰', N'2018-12-11 00:00:00.000', NULL, NULL, N'第二福利院', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'362', N'1083', N'患者家属', N'基本可靠', N'张丁', N'2018-12-25 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'383', N'1105', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-07-23 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'385', N'1107', N'患者本人及家属', N'可信', N'袁纯兰', N'2020-07-31 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'386', N'1108', N'患者家属', N'基本可靠', N'胡新志', N'2020-08-20 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'406', N'1129', N'患者本人', N'基本可靠', N'周婷', N'2021-10-10 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'421', N'1144', N'患者本人及家属', N'可信', N'袁纯兰', N'2022-02-17 00:00:00.000', NULL, NULL, NULL, NULL, N'车凤珍，女，88岁，偶有胸闷胸痛十余年。患者十余年前无明显诱因下出现胸闷胸痛，在松江中心医院就诊，诊断为冠状动脉粥样硬化性心脏病，给予阿司匹林、阿托伐他汀钙片口服后症状仍偶有发作，于4年前在松江区中心医院行支架植入术。后定期给予阿司匹林肠溶片、瑞舒伐他汀等口服，患者胸闷胸痛较前明显好转。2021年7月患者不慎摔倒致左侧股骨颈骨折，未予手术，给予保守治疗，患者因行走困难，生活不能自理于2022年2月17日入住我福利院。发病以来患者无发热，无气急，无咳嗽，无腹痛呕吐等，饮食睡眠可。
患者既往高血压病史20余年，服用珍菊降压片、苯磺酸氨氯地平片控制血压，血压控制尚可，具体数值不详。冠心病术后改服阿利沙坦酯；患者骨质疏松症长期口服骨化三醇软胶囊；因认知障碍、阿尔茨海默病？长期口服石杉碱甲片和多奈哌齐片；因膀胱过度活动症长期口服酒石酸托特罗定缓释片。体检：体温 36.5℃，脉搏75次/分，呼吸20次/分，血压139/85mmHg。两肺呼吸音清，心率75次/分，律齐，腹部软，无压痛，四肢无水肿，左下肢肌力为4级,右下肢肌力为5级。生化:低密度脂蛋白：2.06mmol/L，甘油三酯：1.05mmol/L，高密度脂蛋白：1.62mmol/L，总胆固醇：4.17mmol/L。碱性磷酸酶（ALP）：134.19u/L。心电图：窦性心动过速，电轴左偏，STT改变。B超：胆囊炎、胆囊肿大、胆囊结石。CT：老年脑改变，右肺上中叶多发实性结节，左肺下叶小钙化灶，主动脉及冠状动脉硬化，冠状动脉支架植入中，双侧多发性肋骨陈旧性骨折。（2022.2.11松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'424', N'1148', N'患者家属', N'可信', N'庄秋丽', N'2022-02-25 00:00:00.000', NULL, NULL, NULL, NULL, N'  顾雪娟，女，72岁，右侧肢体瘫痪8个多月。患者去年6月份突然出现右侧肢体瘫痪，言语欠缺，不能正常交流，2021年6月25日头颅CT：双侧基底节及放射冠区腔隙灶，右侧额叶软化灶可能，脑白质变性，老年脑。目前双下肢无力，长期卧床，基本生活不能自理。患者自去年6月份起反反复复出现尾骶部褥疮，目前尾骶部褥疮存在，无红肿感染。患者2年前多次出现行走不稳跌倒，以致两侧股骨颈骨折，于2020年5月及2021年5月先后2次于松江中心医院予以行左右人工髋关节置换。患者原有高血压病约20年，以往曾有头晕不适症状，最高血压约180/110mmHg，常服用降压药治疗，据说服用不规律，故控制不理想。患者目前一般情况尚可，无发热，纳食可，两便无异常。
  患者原有脑梗史8月，高血压病史20年，否认曾有糖尿病、冠心病、慢性支气管炎等其他慢性病史。
  体检：体温 36.7℃，脉搏83次/分，呼吸20次/分，血压117/84mmHg。神清，两肺呼吸音清，无干啰音、无哮鸣音、无湿啰音。83次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区未闻及杂音，无心包摩擦音。四肢无水肿，无肌肉萎缩，肌张力无异常，右侧肢体偏瘫,左上肢肌力为4级,左下肢肌力为1级,右上肢肌力为0级,右下肢肌力为0级。生化:葡萄糖：5.60mmol/L。Rbc：5.08×10<sup>12</sup>/L，Hb：146g/L，Wbc：7.88×10<sup>9</sup>/L，中性粒比例：58.70%，淋巴细胞比例：35.20%，Plt：351×10<sup>9</sup>/L。糖化血红蛋白：5.80%，低密度脂蛋白：1.89mmol/L，甘油三酯：1.34mmol/L，高密度脂蛋白：1.06mmol/L，总胆固醇：3.61mmol/L。Ca：2.40mmol/L，Cl：105.97mmol/L，K：4.02mmol/L，Na：146.43mmol/L，P：1.13mmol/L。A/G：1.37，白蛋白：41.33g/L，谷丙转氨酶：11.90u/L，间接胆红素：8.87umol/L，碱性磷酸酶（ALP）：97.71u/L，总胆红素：13.19umol/L，总蛋白：71.57g/L。球蛋白：30.24g/L。直接胆红素：4.32umol/L。肌酐：52.20umol/L，尿素氮：6.43mmol/L，尿酸：244.83umol/L，心电图：窦性心律，心电轴左偏，顺钟向转位。B超：胆囊术后，残留胆囊管扩张伴结石、胆总管扩张。双肾结石、左肾轻度积水。肝脏、胰腺、脾脏未见明显占位。CT：双侧基底节及放射冠腔隙灶，右侧额叶软化灶。脑白质变性，老年脑。双肺多发小结节。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'436', N'1161', N'患者家属', N'基本可信', N'周婷', N'2022-09-08 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'449', N'1187', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦林，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、穿脱衣服、洗浴等均无法独立完成，进食、行走、如厕可独立完成，本次入院体检提示高脂血症、胆囊结石、脂肪肝。体检：体温 36.3℃，脉搏75次/分，呼吸20次/分，血压99/70mmHg。两肺呼吸音粗，心率75次/分，心律齐，腹软，无压痛，四肢无水肿，左手残疾。生化:葡萄糖：5.2mmol/L。Rbc：5.04×10<sup>12</sup>/L，Hb：143g/L，Wbc：4.91×10<sup>9</sup>/L，中性粒比例：47.8%，淋巴细胞比例：34.7%，Plt：145×10<sup>9</sup>/L。低密度脂蛋白：4.28mmol/L，甘油三酯：1.95mmol/L，高密度脂蛋白：1.37mmol/L，总胆固醇：6.39mmol/L。谷丙转氨酶：28u/L，碱性磷酸酶（ALP）：81u/L，总胆红素：8.6umol/L，肌酐：96umol/L，尿素氮：7.68mmol/L，尿酸：343umol/L，心电图：窦性心律。B超：胆结石、脂肪肝（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'373', N'1095', N'患者本人及家属', N'基本可靠', N'张丁', N'2019-09-25 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'398', N'1120', N'患者家属', N'基本可靠', N'胡新志', N'2021-04-23 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'409', N'1132', N'患者家属', N'基本可信', N'周婷', N'2021-10-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'447', N'1173', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'方志妹，女，82岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙无法完成，穿脱衣、洗浴、如厕方面需要辅助完成，进食、行走方面可以独立完成。入院前体检提示血脂偏高。体检：体温 36.5℃，脉搏92次/分，呼吸19次/分，血压121/91mmHg。两肺呼吸音粗。心率92次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.84mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：4.37×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：134g/L，Wbc：4.81×10<sup>9</sup>/L，中性粒比例：41.7%，淋巴细胞比例：45.7%，Plt：261×10<sup>9</sup>/L。低密度脂蛋白：3.84mmol/L，甘油三酯：1.17mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：1.72mmol/L，总胆固醇：6.23mmol/L。谷丙转氨酶：16u/L（南京艾迪康医学检验所，2022-10-10），碱性磷酸酶（ALP）：81u/L，总胆红素：9.6umol/L，肌酐：65umol/L，尿素氮：5.90mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：224umol/L，心电图：窦性心律，T波改变（上海市上农医院，2022-10-10）X线胸片：正常（上海市上农医院，2022-10-10）B超：胆囊未显示（上海市上农医院，2022-10-10）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'482', N'1212', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民远，男，69岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。有慢性支气管炎伴肺气肿、支气管哮喘史。有乙型肝炎、肺结核史。体检：体温 36.5℃，脉搏92次/分，呼吸20次/分，血压117/76mmHg。两肺呼吸音低，无干湿。心率92次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.49mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.66×10<sup>12</sup>/L，Hb：142g/L，Wbc：7.7×10<sup>9</sup>/L，中性粒比例：70.4%，淋巴细胞比例：20.4%，Plt：283×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：3.60mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：0.66mmol/L，高密度脂蛋白：1.52mmol/L，总胆固醇：5.39mmol/L。谷丙转氨酶：12u/L，碱性磷酸酶（ALP）：131u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：9.5umol/L，肌酐：60umol/L，尿素氮：4.64mmol/L，尿酸：299umol/L（南京艾迪康医学检验所，2022-10-12），心电图：正常（上海市上农医院，2022-10-12）X线胸片：两肺陈旧性结核，两侧肋膈角变钝。（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'504', N'1223', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'贾秀华0867，女，69岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣、洗漱、进食、行走、如厕等可独立完成。入院体检提示：脂肪肝、高脂血症。患者目前精神状态可，食欲、大小便、睡眠较正常。体检：体温 37.3℃，脉搏95次/分，呼吸20次/分，血压134/85mmHg。两肺呼吸音清，95次/分，心律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.19mmol/L（2022-10-14上海市上农医院）。Rbc：4.77×10<sup>12</sup>/L，Hb：141g/L，Wbc：6.88×10<sup>9</sup>/L，中性粒比例：57.4%，淋巴细胞比例：30.8%，Plt：163×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.80mmol/L（2022-10-14上海市上农医院），甘油三酯：1.51mmol/L，高密度脂蛋白：1.55mmol/L，总胆固醇：6.09mmol/L。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：102u/L（2022-10-14上海市上农医院），总胆红素：14.5umol/L，肌酐：58umol/L，尿素氮：5.15mmol/L，尿酸：304umol/L（2022-10-14上海市上农医院），心电图：窦性心律 左室肥大 T波改变（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：脂肪肝（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'505', N'1222', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆珠0862，女，84岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中进食、穿脱衣服、如厕、洗浴等需协助完成，行走缓慢。患者因高血压病目前服用复方卡托普利片。体检：体温 36.7℃，脉搏80次/分，呼吸20次/分，血压107/73mmHg。两肺呼吸音粗，无干湿啰音。心率80次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.86mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.49×10<sup>12</sup>/L，Hb：129g/L，Wbc：4.45×10<sup>9</sup>/L，中性粒比例：64.4%，淋巴细胞比例：24.6%，Plt：129×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：3.29mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：1.70mmol/L，高密度脂蛋白：1.30mmol/L，总胆固醇：5.27mmol/L。谷丙转氨酶：11u/L，碱性磷酸酶（ALP）：60u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：10.6umol/L，肌酐：83umol/L，尿素氮：6.62mmol/L，尿酸：433umol/L（南京艾迪康医学检验所，2022-10-14），心电图：房速，T波低平（上海市上农医院，2022-10-14）X线胸片：右肺陈旧性病灶（条带状致密影）（上海市上农医院，2022-10-14）B超：胆囊未显示（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'506', N'1224', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'高秀英0868，女，76岁，智力低下数十年患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等能自理完成。入院前体检提示高脂血症。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核等传染病史，有梅毒史。体检：体温 36.6℃，脉搏94次/分，呼吸20次/分，血压130/87mmHg。两肺呼吸音清，94次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.50mmol/L（2022-10-14上海市上农医院）。Rbc：4.76×10<sup>12</sup>/L，Hb：153g/L，Wbc：7.26×10<sup>9</sup>/L，中性粒比例：62%，淋巴细胞比例：32.3%，Plt：203×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.84mmol/L（2022-10-14上海市上农医院），甘油三酯：1.49mmol/L，高密度脂蛋白：1.44mmol/L，总胆固醇：5.97mmol/L。谷丙转氨酶：16u/L，碱性磷酸酶（ALP）：87u/L（2022-10-14上海市上农医院），总胆红素：15.4umol/L，肌酐：64umol/L，尿素氮：7.13mmol/L，尿酸：249umol/L（2022-10-14上海市上农医院），心电图：正常心电图（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'507', N'1216', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆姝0841，女，63岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、如厕、洗浴等无法独立完成。患者目前服用盐酸氯丙嗪片。本次入院体检提示高脂血症、脂肪肝、心肌供血不足。无高血压病、糖尿病史体检：体温 36.6℃，脉搏104次/分，呼吸20次/分，血压99/72mmHg。两肺呼吸音粗，无干湿啰音。心率104次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.65mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.36×10<sup>12</sup>/L，Hb：136g/L，Wbc：6.53×10<sup>9</sup>/L，中性粒比例：67.7%，淋巴细胞比例：24.8%，Plt：240×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：3.53mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：1.59mmol/L，高密度脂蛋白：1.36mmol/L，总胆固醇：5.73mmol/L。谷丙转氨酶：27u/L，碱性磷酸酶（ALP）：83u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：8.4umol/L，肌酐：71umol/L，尿素氮：5.63mmol/L，尿酸：322umol/L（南京艾迪康医学检验所，2022-10-14），心电图：窦性心律，T波改变（上海市上农医院，2022-10-14） X线胸片：正常（上海市上农医院，2022-10-14） B超：脂肪肝（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'532', N'1203', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民保0135，男，62岁，智力低下几十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下。日常生活中洗脸、刷牙、进食、行走、穿脱衣、洗浴、如厕等需要帮助。入院前体检提示：窦性心律、ST段改变。患者目前精神状态可，食欲、大小便均无异常。体检：体温 36.3℃，脉搏78次/分，呼吸20次/分，血压114/79mmHg。两肺呼吸音粗，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。78次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.07(南京艾迪康医学检验所)mmol/L。Rbc：4.7×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：153g/L，Wbc：6.04×10<sup>9</sup>/L，中性粒比例：71.3%，淋巴细胞比例：19.4%，Plt：164×10<sup>9</sup>/L。低密度脂蛋白：2.44mmol/L，甘油三酯：0.45mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.46mmol/L，总胆固醇：4.21mmol/L。谷丙转氨酶：12u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：84u/L，总胆红素：11umol/L，肌酐：70umol/L，尿素氮：7.09mmol/L（南京艾迪康医学检验所），尿酸：271umol/L，心电图：1.窦性心律 2.ST段改变X线胸片：（-）B超：（-）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'533', N'1250', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民高0972，男，68岁，智力低下数十年。患者为上海市救助二站受助人员，今入住本院。患者自幼智力低下。日常生活中除洗浴需要协助，洗脸刷牙、进食、行走、穿脱衣、如厕等均可自理。入院前心电图检查：窦性心动过速。患者目前精神状态可，食欲、大小便较正常。体检：体温 36.6℃，脉搏107次/分，呼吸20次/分，血压112/83mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。107次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.94mmol/L（南京艾迪康医学检验所）。Rbc：4.87×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：146g/L，Wbc：5.84×10<sup>9</sup>/L，中性粒比例：72.6%，淋巴细胞比例：22.6%，Plt：231×10<sup>9</sup>/L。低密度脂蛋白：2.68mmol/L，甘油三酯：0.81mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.43mmol/L，总胆固醇：4.48mmol/L。谷丙转氨酶：18u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：84u/L，总胆红素：7.1umol/L，肌酐：57umol/L，尿素氮：5.36mmol/L（南京艾迪康医学检验所），尿酸：267umol/L，心电图：窦性心动过速X线胸片：（-）B超：（-）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'534', N'1260', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民才1158，男，68岁，智力低下数十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下。日常生活中可独立完成洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等。患者因高血压在服硝苯地平片，血压控制较稳定。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高：心电图检查：窦性心律、ST段改变。患者目前精神状态可，食欲、大小便较正常。体检：体温 36.6℃，脉搏84次/分，呼吸20次/分，血压122/81mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。84次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.4mmol/L（南京艾迪康医学检验所）。Rbc：4.69×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：138g/L，Wbc：8.68×10<sup>9</sup>/L，中性粒比例：66.7%，淋巴细胞比例：22.7%，Plt：353×10<sup>9</sup>/L。低密度脂蛋白：3.77mmol/L，甘油三酯：1.51mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.48mmol/L，总胆固醇：5.77mmol/L。谷丙转氨酶：25u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：122u/L，总胆红素：4.9umol/L，肌酐：75umol/L，尿素氮：5.03mmol/L（南京艾迪康医学检验所），尿酸：348umol/L，心电图：窦性心律、ST段改变。X线胸片：两肺紊乱增多增粗紊乱B超：（-）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'538', N'1261', N'患者家属', N'基本可信', N'庄秋丽', N'2023-03-02 00:00:00.000', NULL, NULL, NULL, NULL, N'黄桂珍，女，94岁，儿子代诉：两下肢无力3月。据其儿子诉患者10余年前曾有脑梗塞史，当时发现其两下肢无力，后于松江中心医院就诊，具体家属诉不详，后完全康复，基本生活都能自理。2022年12月29日患有新冠后，两下肢不能独自站立行走，需帮助搀扶行走，基本生活不能自理。
患者原有原发性高血压病20余年，最高血压不详，常服用高血压药控制血压，据说控制理想。
患者原有冠状动脉粥样硬化性心脏病3年（医保记录卡有记录），有心衰症状，两下肢反复水肿，常服用托拉塞米及麝香保心丸。
患者原有贫血2年，患者此次入院前体检：红细胞3.73*10~12/L，血红蛋白74.00g\L。
患者此次入院前体检：CT：双侧放射冠区腔隙灶，老年脑改变，双侧部分肋骨骨折改变，骨痂形成，慢性支气管炎样改变、肺气肿，伴有两肺内散在慢性炎症。B超：双肾弥漫性病变，右肾囊肿。心电图：窦性心动过速。
患者目前一般情况尚可，无发热，纳食可，两便无异常。
体检：体温 36.5°C℃，脉搏109次/分，呼吸20次/分，血压133/80mmHgmmHg。神清，精神可，听力严重障碍，反应迟钝，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音粗，未及明显干湿啰音，心率109次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢无水肿，左上肢肌力为5级，左下肢肌力为4级，右上肢肌力为5级，右下肢肌力为4级。生化:Rbc：3.73×10<sup>12</sup>/L，Hb：74g/L，Wbc：4.83×10<sup>9</sup>/L，中性粒比例：53%，淋巴细胞比例：30.20%，Plt：297×10<sup>9</sup>/L。糖化血红蛋白：5.30%，低密度脂蛋白：1.70mmol/L，甘油三酯：0.80mmol/L，高密度脂蛋白：2.23mmol/L，总胆固醇：5.11mmol/L。Ca：2.27mmol/L，Cl：102.60mmol/L，K：4.56mmol/L，Na：138.69mmol/L，P：1.26mmol/L。A/G：1.43，白蛋白：39.37g/L，谷丙转氨酶：4.19u/L，间接胆红素：3.29umol/L，碱性磷酸酶（ALP）：58.63u/L，总胆红素：5.79umol/L，总蛋白：66.98g/L。球蛋白：27.61g/L。直接胆红素：2.50umol/L。肌酐：75.22umol/L，尿素氮：5.83mmol/L，尿酸：284.08umol/L，心电图：窦性心动过速X线胸片：(缺)。B超：双肾弥漫性病变，右肾囊肿CT：双侧放射冠区腔隙灶，老年脑，双侧部分肋骨骨折改变，骨痂形成，慢性支气管炎样改变、肺气肿，伴有两肺内散在慢性炎症。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'547', N'1270', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-04-19 00:00:00.000', NULL, NULL, N'', N'', N'阙介福，男，89岁，活动后气促4年余。患者十年来时有活动后气促，经休息后症状缓解，未予诊断和治疗。四年来症状逐渐加重，轻微活动即可发生气促不适，在松江区中心医院就诊，诊断为“冠心病”，给予呋塞米和和螺内酯片后症状好转，后按时服药病情趋于稳定。因年事高，家中无人照顾于2023年04月19入住本院。患者慢性支气管炎病史40余年，长期吸氧治疗。本次入院体检提示胆囊炎、胆囊结石。发病以来患者无发热，无头晕头痛，无胸闷，无呕吐腹泻等不适。否认肝炎、结核病史，腰椎间盘突出症术后15年，消化道异物取出术后2年，左眼青光眼术后，慢支病史40余年，既往睡眠障碍病史，服用艾司唑仑片，目前不服药。体检：体温 36.5℃，脉搏74次/分，呼吸20次/分，血压131/74mmHg。两肺呼吸音粗，心率74次/分，心律齐，腹软，无压痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。生化:葡萄糖：5.9mmol/L。Rbc：4.48×10<sup>12</sup>/L，Hb：131g/L，Wbc：7.98×10<sup>9</sup>/L，中性粒比例：41%，淋巴细胞比例：49.7%，Plt：298×10<sup>9</sup>/L。糖化血红蛋白：5.5%，低密度脂蛋白：3.81mmol/L，甘油三酯：2.02mmol/L，高密度脂蛋白：1.11mmol/L，总胆固醇：4.22mmol/L。Cl：101mmol/L。K：4.5mmol/L，Na：139mmol/L，A/G：1.3，白蛋白：39g/L，谷丙转氨酶：25u/L，间接胆红素：4.4umol/L，碱性磷酸酶（ALP）：65.6u/L，总胆红素：5.5umol/L，总蛋白：69g/L。球蛋白：30g/L。直接胆红素：1.1umol/L。肌酐：66.7umol/L，尿素氮：5.55mmol/L，尿酸：311umol/L，心电图：窦性心律。B超：肝内钙化灶，胆囊炎，胆囊胆固醇结晶，胆囊结石，双肾偏小，胰腺、脾脏未见明显占位。CT：老年脑改变，肺部未见明显异常，主动脉、冠状动脉硬化（2023.4.14松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'549', N'1273', N'患者家属', N'可靠', N'袁纯兰', N'2023-04-25 00:00:00.000', NULL, NULL, N'', N'', N'李秀凤，女，84岁，时有反酸、嗳气六年余。患者原有慢性胃炎病史六余年，时有反酸、嗳气等不适，给予奥美拉唑肠溶胶囊口服后好转。目前因患者年事渐高，家中无人照顾，于2023年04月25号入住本院。本次入院体检提示脑梗死，带来培元通脑胶囊口服。患者因便秘目前服用舒秘胶囊，患者发病以来无发热，无胸闷气急，饮食、睡眠尚可，大小便正常。慢性阻塞性肺病、哮喘20余年，慢性胃炎6年余，便秘6年余，淋巴癌15年，胸椎骨折术后，股骨骨折术后体检：体温 36.8℃，脉搏82次/分，呼吸20次/分，血压121/66mmHg。两肺呼吸音粗，心率82次/分，心律齐，腹软，无压痛，右臀部皮肤损伤伴变红。生化:葡萄糖：5.01mmol/L。Rbc：3.64×10<sup>12</sup>/L，Hb：81g/L，Wbc：4.81×10<sup>9</sup>/L，中性粒比例：65.5%，淋巴细胞比例：23.5%，Plt：270×10<sup>9</sup>/L。糖化血红蛋白：5.9%，低密度脂蛋白：2.54mmol/L，甘油三酯：1.12mmol/L，高密度脂蛋白：1.3mmol/L，总胆固醇：4.19mmol/L。Ca：2.03mmol/L，Cl：97.69mmol/L，K：4.02mmol/L，Na：134.21mmol/L，P：1.09mmol/L。白蛋白：30.69g/L，谷丙转氨酶：7.39u/L，碱性磷酸酶（ALP）：120.86u/L，总胆红素：12.87umol/L，总蛋白：73.32g/L。肌酐：43.6umol/L，尿素氮：4.81mmol/L，尿酸：181.77umol/L。心电图：窦性心动过速，不完全性右束支传导阻滞。B超：胆囊结石，肝脏、胰腺、脾脏、双肾未见明显占位。CT：慢性支气管病变，双飞下叶少许慢性炎症，左肺上叶多发结节，主动脉及冠状动脉硬化，下段胸椎术后内固定中，两侧基底节及放射冠区多发腔隙灶，老年脑，脑白质变性（2023.4.18松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'550', N'1272', N'患者家属', N'', N'袁纯兰', N'2023-04-24 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'596', N'1319', N'患者家属', N'基本可靠', N'胡新志', N'2024-04-09 00:00:00.000', NULL, NULL, N'', N'', N'徐勤芳，女，88岁，记忆力进行性减退一年。患者一年前开始出现记忆力进行性减退，丢三落四、易忘事，总觉得自己东西被偷，时有藏东西行为，无猜疑、被害妄想。偶有情绪不稳定，无明显吵闹、纠缠不清。2024年3月8突然不认识家人，不愿说话，不愿下床活动，不知道吃饭，在松江精神卫生中心诊断“老年性痴呆症”。无药物治疗。目前因年事已高、个人生活不能自理于20240410自愿入住我院。患者一般情况尚可，轮椅推入病房，表情淡漠，简单对答，听力减退，查体合作，饮食可、睡眠可，大小便正常。
患者因高血压、脑梗死后遗症目前长期口服硝苯地平片、血塞通片。否认肝炎、结核传染病史，否认糖尿病史体检：体温 36.5℃，脉搏18次/分，呼吸63次/分，血压140/67mmHgmmHg。两肺呼吸音清，63次/分，心律齐，无腹壁紧张，无压痛。右上腹部扪及一直径约5cmx6cm左右肿块，无压痛，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：5.76mmol/L，糖化血红蛋白：5.20%，低密度脂蛋白：3.43mmol/L，甘油三酯：1.13mmol/L，高密度脂蛋白：1.27mmol/L，总胆固醇：4.99mmol/L。Ca：2.00mmol/L，Cl：109.20mmol/L，K：3.57mmol/L，Na：141.54mmol/L，P：0.77mmol/L，A/G：1.27，白蛋白：32.90g/L，谷丙转氨酶：6.31u/L，间接胆红素：5.44umol/L，碱性磷酸酶（ALP）：128.41u/L，总胆红素：11.13umol/L，总蛋白：58.90g/L。球蛋白：26.00g/L。直接胆红素：5.69umol/L。肌酐：104.85umol/L，尿素氮：5.66mmol/L，尿酸：263.66umol/L，心电图：窦性心律 T波改变 V4-V6低平，B超：血吸虫肝病 胆囊壁水肿、胆囊胆固醇结晶 脾内钙化灶 双肾缩小伴弥漫性病变、右肾小结石、左肾小囊肿 胰腺显示不清。附见：上腹部实性占位、腹水（少量）CT：头颅CT:左枕叶片状低密度影，老年脑改变. 胸部CT:1.两肺间质性肺水肿，双侧胸腔积液，心脏增大，主动脉及冠状动脉壁硬化：慢性心功能不全表现2.右腋前区、颈根部及纵膈内多发钙化灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'609', N'1332', N'患者家属', N'可靠', N'王屹', N'2024-05-23 00:00:00.000', NULL, NULL, N'', N'', N'谢国君，男，87岁，时有头晕不适二十余年。患者二十余年前经常出现头晕头痛不适，经医院诊断为：高血压，予降压药治疗后血压基本稳定。目前患者不规则服药，未定期监测血压。患者一月前不慎跌倒，至脑挫伤、颅骨和面股多发性骨折。患者口齿不清，认知功能下降，生活自理能力逐步减退，长期卧床。患者原有老年痴呆症三年余予吡拉西坦分散片治疗；原有冠心病数年予血塞通片治疗；原有慢性支气管炎肺气肿数年予间断吸氧。本次入院体检：左侧耻骨上下支陈旧性骨折；右侧第10、12肋骨陈旧性骨折；心房颤动；脑白质变性，老年脑改变；血吸虫性肝病；低血钾。患者目前精神状态尚可，食欲、大小便较正常，睡眠欠佳。今由家属送至本院住养。
体检：体温 36.8℃，脉搏82次/分，呼吸18次/分，血压129/75mmHg。两肺呼吸音粗，心率82次/分，心律不齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，双下肢散在褐色斑块状皮疹。肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：随机7.86
mmol/L（2024.4.17松江区中心医院）。Rbc：5.59×10<sup>12</sup>/L，Hb：173g/L，Wbc：7.59×10<sup>9</sup>/L，中性粒比例：73.9%，Plt：144
×10<sup>9</sup>/L（2024.5.7松江区洞泾镇社区卫生服务中心）。Ca：2.16
mmol/L（2024.4.17松江区中心医院），Cl：102mmol/L，K：3.39mmol/L，Na：141mmol/L，A/G：1.96，白蛋白：45.2g/L，谷丙转氨酶：14u/L，间接胆红素：23.4umol/L，碱性磷酸酶（ALP）：103
u/L（2024.4.17松江区中心医院），总胆红素：34.8umol/L，总蛋白：68.3g/L。直接胆红素：11.4umol/L。肌酐：91umol/L，尿素氮：4.5mmol/L，尿酸：342
umol/L（2024.4.17松江区中心医院），心电图：心房颤动
（2024.4.17松江区中心医院）X线胸片：(缺)。B超：血吸虫性肝病
（2024.4.30松江区洞泾镇社区卫生服务中心）CT：脑白质变性，老年脑改变；左侧耻骨上下支陈旧性骨折；右侧第10、12肋骨陈旧性骨折；慢性支气管炎，肺气肿；心脏增大，主动脉硬化
（2024.4.17松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'388', N'1110', N'患者家属', N'基本可靠', N'胡新志', N'2020-09-11 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'511', N'1228', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'刘长英0887，女，66岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴等方面能够独立完成。患者因高血压病目前服用硝苯地平片、复方卡托普利片。本次入院前体检提示总胆固醇偏高。体检：体温 36.5℃，脉搏76次/分，呼吸18次/分，血压109/74mmHg。两肺呼吸音粗，无干湿啰音。心率76次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.91mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.46×10<sup>12</sup>/L，Hb：132g/L，Wbc：4.47×10<sup>9</sup>/L，中性粒比例：66.0%，淋巴细胞比例：27.0%，Plt：209×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：2.96mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：0.85mmol/L，高密度脂蛋白：1.86mmol/L，总胆固醇：5.38mmol/L。谷丙转氨酶：15u/L，碱性磷酸酶（ALP）：81u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：8.4umol/L，肌酐：75umol/L，尿素氮：10.08mmol/L，尿酸：246umol/L（南京艾迪康医学检验所，2022-10-14），心电图：窦性心律，电轴左偏（上海市上农医院，2022-10-14）  X线胸片：右侧肋膈角变钝，余无殊（上海市上农医院，2022-10-14）  B超：正常（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'512', N'1236', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆怡，女，69岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示血糖、胆固醇偏高。既往乙型病毒性肝炎病史，否认结核病史体检：体温 36.4℃，脉搏73次/分，呼吸20次/分，血压140/83mmHg。两肺呼吸音清，心率73次/分，律齐，腹软，无压痛，四肢无水肿，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.84mmol/L（上海市上农医院2022.10.14）。Rbc：4.74×10<sup>12</sup>/L，Hb：149g/L，Wbc：5.05×10<sup>9</sup>/L，中性粒比例：60.3%，淋巴细胞比例：35.4%，Plt：250×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：3.47mmol/L，甘油三酯：0.91mmol/L，高密度脂蛋白：1.79mmol/L，总胆固醇：5.83mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：13u/L，碱性磷酸酶（ALP）：98u/L，总胆红素：14.1umol/L（上海市上农医院2022.10.14），肌酐：51umol/L，尿素氮：3.99mmol/L，尿酸：183umol/L（上海市上农医院2022.10.14），心电图：正常心电图（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'515', N'1231', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆秋0891，女，69岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱无法完成，穿脱衣服、洗浴需要辅助完成，进食、行走、如厕可独立完成。因精神发育迟滞曾在浦东新区精神卫生中心住院治疗。本次入院体检提示高脂血症、脂肪肝。患者目前精神状态可，食欲、大小便、睡眠较正常。否认结核、肝炎传染病史。体检：体温 37.3℃，脉搏104次/分，呼吸20次/分，血压120/81mmHg。两肺呼吸音清，104次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：10.79mmol/L（2022-10-14上海市上农医院）。Rbc：5.09×10<sup>12</sup>/L，Hb：153g/L，Wbc：7.73×10<sup>9</sup>/L，中性粒比例：58.6%，淋巴细胞比例：33.0%，Plt：205×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：4.33mmol/L（2022-10-14上海市上农医院），甘油三酯：2.20mmol/L，高密度脂蛋白：1.41mmol/L，总胆固醇：5.58mmol/L。谷丙转氨酶：46u/L，碱性磷酸酶（ALP）：87u/L（2022-10-14上海市上农医院），总胆红素：17.7umol/L，肌酐：45umol/L，尿素氮：4.66mmol/L，尿酸：206umol/L（2022-10-14上海市上农医院），心电图：正常心电图（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：脂肪肝 胆囊未显示（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'519', N'1238', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'张顺芝0922，女，75岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴等能够独立完成。本次入院体检提示心肌供血不足、肾功能减退、胆囊结石。体检：体温 37.1℃，脉搏90次/分，呼吸20次/分，血压143/81mmHg。两肺呼吸音清。心率90次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.71mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.37×10<sup>12</sup>/L，Hb：89g/L，Wbc：8.59×10<sup>9</sup>/L，中性粒比例：86.3%，淋巴细胞比例：9.3%，Plt：305×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：2.42mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：0.96mmol/L，高密度脂蛋白：1.36mmol/L，总胆固醇：4.27mmol/L。谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：126u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：19.0umol/L，肌酐：89umol/L，尿素氮：6.64mmol/L，尿酸：327umol/L（南京艾迪康医学检验所，2022-10-14），心电图：窦性心律，T波低平（V1-V6），不排除心肌缺血（上海市上农医院，2022-10-14）X线胸片：心影增大，余无殊（上海市上农医院，2022-10-14）  B超：胆结石（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'521', N'1230', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'金志娥0890，女，75岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、洗浴、行走、如厕等方面需协助完成。患者因高血压病、心肌缺血目前服用厄贝沙坦氢氯噻嗪片、复方丹参片等。有高脂血症、尿酸偏高史，有梅毒、乙型肝炎史。体检：体温 36.1℃，脉搏65次/分，呼吸17次/分，血压139/70mmHg。两肺呼吸音清。心率65次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.97mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：3.99×10<sup>12</sup>/L，Hb：121g/L，Wbc：5.33×10<sup>9</sup>/L，中性粒比例：60.1%，淋巴细胞比例：29.8%，Plt：155×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：3.44mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：2.16mmol/L，高密度脂蛋白：1.20mmol/L，总胆固醇：5.69mmol/L。谷丙转氨酶：14u/L，碱性磷酸酶（ALP）：85u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：7.9umol/L，肌酐：78umol/L，尿素氮：8.4mmol/L，尿酸：367umol/L（南京艾迪康医学检验所，2022-10-14），心电图：窦性心律，T波低平、倒置（V1-V6），不排除前侧壁心肌缺血（上海市上农医院，2022-10-14） X线胸片：主动脉结钙化，余无殊（上海市上农医院，2022-10-14） B超：正常（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'522', N'1259', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民柱1052，男，65岁，智力低下数十年。患者为上海市救助二站受助人员，今入住本院。患者疑似精神障碍，能听懂语言，但无法交流，日常吃饭、洗澡、穿衣、如厕等方面能够完全自理。入院前体检血脂偏高、肝功能异常；心电图提示心律不齐。因时有出现伤人、推人等过激行为长期服用奥氮平片。目前一般情况稳定，无发热，无头晕头痛等不适，纳可，大小便正常。无结核、血吸虫等传染病史，有肝炎史.体检：体温 36.4℃，脉搏68次/分，呼吸20次/分，血压115/74mmHg。两肺呼吸音粗，无干湿啰音、无哮鸣音、无胸膜摩擦音、无异常呼吸音、语音传导无异常。68次/分，心律不齐，第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.11mmol/L（南京艾迪康医学检验所）。Rbc：5.26×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：157g/L，Wbc：6.42×10<sup>9</sup>/L，中性粒比例：60.5%，淋巴细胞比例：30.7%，Plt：179×10<sup>9</sup>/L。低密度脂蛋白：3.91mmol/L，甘油三酯：1.28mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.39mmol/L，总胆固醇：5.64mmol/L。谷丙转氨酶：42u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：111u/L，总胆红素：14.3umol/L，肌酐：81umol/L，尿素氮：5.07mmol/L（南京艾迪康医学检验所），尿酸：252umol/L，心电图：窦性心律。心律不齐；X线胸片：两下肺纹理增粗。紊乱B超：未见明显异常。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'523', N'1247', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'黄水亭0948，女，67岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示肺结核病史，乙型病毒性肝炎和心悸供血不足等。否认食物药物过敏史。体检：体温 36.6℃，脉搏88次/分，呼吸20次/分，血压125/80mmHg。两肺呼吸音粗，心率88次/分，心律齐，腹软，无压痛。四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.93mmol/L（上海市上农医院2022.10.14）。Rbc：4.89×10<sup>12</sup>/L，Hb：148g/L，Wbc：6.49×10<sup>9</sup>/L，中性粒比例：58.9%，淋巴细胞比例：32.6%，Plt：240×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：2.71mmol/L，甘油三酯：0.98mmol/L，高密度脂蛋白：1.51mmol/L，总胆固醇：4.82mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：78u/L，总胆红素：11.4umol/L（上海市上农医院2022.10.14），肌酐：67umol/L，尿素氮：7.81mmol/L，尿酸：278umol/L（上海市上农医院2022.10.14），心电图：窦性心律，T波地平（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）CT：梅毒特异性抗体（-）（上海市上农医院2022.10.14）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'524', N'1248', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆雨0949，女，85岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示窦性心动过缓和贫血。否认肝炎、结核病史。体检：体温 36.9℃，脉搏85次/分，呼吸20次/分，血压113/79mmHg。两肺呼吸音粗，心率85次/分，心律齐，腹软，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.68mmol/L（上海市上农医院2022.10.14）。Rbc：4.41×10<sup>12</sup>/L，Hb：97g/L，Wbc：3.13×10<sup>9</sup>/L，中性粒比例：62.3%，淋巴细胞比例：29.6%，Plt：272×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：2.74mmol/L，甘油三酯：0.79mmol/L，高密度脂蛋白：1.79mmol/L，总胆固醇：5.01mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：15u/L，碱性磷酸酶（ALP）：89u/L，总胆红素：8.3umol/L（上海市上农医院2022.10.14），肌酐：50umol/L，尿素氮：4.1mmol/L，尿酸：179umol/L（上海市上农医院2022.10.14），心电图：窦性心动过缓（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'525', N'1201', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'金福娣0037，女，63岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示有梅毒，窦性心动过缓，血压偏高，血脂偏高等。否认肝炎、结核病史。体检：体温 35.9℃，脉搏78次/分，呼吸20次/分，血压120/81mmHg。两肺呼吸音粗，心率78次/分，心律齐，腹软，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.55mmol/L（上海市上农医院2022.10.14）。Rbc：4.11×10<sup>12</sup>/L，Hb：120g/L，Wbc：8.4×10<sup>9</sup>/L，中性粒比例：53.5%，淋巴细胞比例：39.3%，Plt：186×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：3.87mmol/L，甘油三酯：1.19mmol/L，高密度脂蛋白：1.78mmol/L，总胆固醇：6.5mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：79u/L，总胆红素：7.2umol/L（上海市上农医院2022.10.14），肌酐：57umol/L，尿素氮：9.46mmol/L，尿酸：234umol/L（上海市上农医院2022.10.14），心电图：窦性心动过缓，左室高电压，T波高值（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'540', N'1263', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-03-16 00:00:00.000', NULL, NULL, N'', N'', N'王双全，男，81岁，双下肢乏力近6年入院。患者6年前无明显诱因下出现双下肢乏力，在上海市第一人民医院南院就诊，诊断为“脑梗死”，给予口服药和康复治疗，仍双下肢乏力，生活不能完全自理，目前长期服用阿司匹林肠溶片和阿托伐他汀钙片治疗。患者原有高血压病约20年，规律服用氯沙坦钾片，血压控制在正常范围，具体数值不详。近一年出现兴奋，答非所问，不知道自己是否吃饭，不知道自己所处地点，记忆力减退明显，在松江区中心医院诊断为认知障碍，目前未有特殊治疗。本次入院体检提示高脂血症、肝囊肿。发病以来患者无发热，无胸闷气急，无头晕头痛，无咳嗽气喘，无腹痛呕吐，纳食可，两便无异常。体检：体温 36℃，脉搏79次/分，呼吸20次/分，血压137/78mmHg。两肺呼吸音清，心率79次/分，心律齐，腹软，无压痛。糖化血红蛋白：4.9%，低密度脂蛋白：1.31mmol/L，甘油三酯：0.98mmol/L，高密度脂蛋白：1.66mmol/L，总胆固醇：3.23mmol/L。白蛋白：44.37g/L，谷丙转氨酶：22.76u/L，间接胆红素：8.55umol/L，碱性磷酸酶（ALP）：9.43u/L，总胆红素：15.45umol/L，总蛋白：70.91g/L。球蛋白：26.54g/L。直接胆红素：6.9umol/L。肌酐：57.91umol/L，尿素氮：3.78mmol/L，尿酸：297.66umol/L。心电图：窦性心律，PR间期正常高值。B超：肝囊肿，胆囊术后未显示，肝内外胆管未见明显扩张。CT：桥脑右份和左侧丘脑区腔隙灶，老年脑改变（2023.3.14松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'568', N'1290', N'患者家属', N'基本可信', N'庄秋丽', N'2023-08-29 00:00:00.000', NULL, NULL, NULL, NULL, N'宋玲弟，女，74岁，患者时胡言乱语3-4年。患者近3-4年期间说话颠三倒四，胡言乱语，记忆力差，问啥事都不知道，自知力认知能力差，本年度7月5日曾于松江泗泾医院CT检查提示：双侧基底节区腔隙灶，老年脑改变。2023年8月21日松江区精神卫生中心诊疗意见：阿尔茨海默症（混合型）。患者1年前不慎跌倒致右肩部疼，右手臂活动受限，目前不能抬举，右手握物无力。当时未就诊，2023年8月22日松江区泗泾医院CT：右肩关节脱位，右肩关节退行性骨关炎。患者原有原发性高血压病20余年，曾最高血压180/130mmHg，平时常服用降压药控制血压，据说血压控制可，目前无头晕头痛等不适。患者双眼视物模糊4年，2021年4月8-10期间曾于上海爱尔眼科医院行右眼晶状体植入手术，手术顺利。但目前两眼视物仍模糊不清。本次入院前体检(2023.8.22松江泗泾医院）B超：肝囊肿，双肾囊肿。胆囊炎，胆囊结石。患者目前一般情况可，无发热，纳食可，两便无异常。体检：体温 36.7℃，脉搏83次/分，呼吸20次/分，血压107/72mmHg神清，精神可，巩膜清，结膜无异常，两眼视物模糊，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，未及明显干湿啰音，心率83次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，四肢无水肿，右肩部压痛，无红肿青紫，右手臂活动受限，左上肢肌力为5级，左下肢肌力为5级，右上肢肌力为2级，右下肢肌力为5级。生化:葡萄糖：6.00mmol/L。Rbc：3.39×10<sup>12</sup>/L，Hb：119g/L，Wbc：7.77×10<sup>9</sup>/L，中性粒比例：73.8%，淋巴细胞比例：18.1%，Plt：250×10<sup>9</sup>/L。C反应蛋白：1.3MG/L，糖化血红蛋白：5.4%，低密度脂蛋白：1.04mmol/L，甘油三酯：0.58mmol/L，高密度脂蛋白：1.15mmol/L，总胆固醇：2.77mmol/L。Ca：2.00mmol/L，Cl：105mmol/L，K：4.29mmol/L，Na：142mmol/L，A/G：1.4，白蛋白：39.5g/L，谷丙转氨酶：41u/L，碱性磷酸酶（ALP）：80u/L，总胆红素：5.1umol/L，总蛋白：68g/L。球蛋白：28.8g/L。直接胆红素：1.4umol/L。肌酐：49.0umol/L，尿素氮：5.30mmol/L，尿酸：187umol/L，心电图：正常窦性心律，电轴左偏。B超：肝囊肿，胆囊炎，胆囊结石，双肾囊肿。CT：双侧基底节区腔隙灶，老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'569', N'1292', N'患者本人及家属', N'可靠', N'庄秋丽', N'2023-08-31 00:00:00.000', NULL, NULL, N'', N'', N'张婉芳，女，89岁，患者慢性咳嗽咳痰10余年，加重10月左右。患者患有慢性阻塞性肺疾病10余年，长期有慢性咳嗽咳痰，79岁时因咳嗽咳痰严重，于上海市松江区中心医院就诊作CT检查时发现：右肺部结节（恶性肿瘤可能），未手术未作明确诊断。于去年冬季起咳嗽咳痰加重，目前咳嗽咳痰较剧，痰呈白粘痰，不易咳出，常服用祛痰止咳药，气不喘，无胸痛咳血等。
患者2年前住院治疗时发现有原发性高血压病，最高血压160/90mmHg，目前常服用降压药，据说血压控制可，目前无头晕头痛等不适。患者排尿困难10余年，10年前曾因此于松江中心医院做过手术治疗（具体不详），但手术效果不佳，目前仍因排尿困难长期服药治疗。患者2023年7月27日-8月19日因头晕不适于松江中心医院住院治疗，住院期间予以头颅MRI（2023.8.1）提示：双侧额部硬膜下积液，双侧放射冠区多发缺血灶，老年脑改变。B超：肝脏轻度弥漫性病变、肝脏囊肿，肝内钙化灶。出院诊断除上述疾病，另有诊断：睡眠障碍，焦虑状态，下肢肌间静脉血栓形成，支气管扩张伴感染。患者目前精神可，无发热，纳食欠佳，咳嗽咳痰，痰呈白黏痰，不易咳出，气不喘，排尿困难，想尿尿不了尿不尽感觉，大便难解常用开塞露通便，服用利眠药后睡眠可。体检：体温 36.9℃，脉搏92次/分，呼吸20次/分，血压151/90mmHg。神清，精神可，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音粗，未及明显干湿啰音，心率92次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢无水肿。生化:葡萄糖：4.62mmol/L。Rbc：3.59×10<sup>12</sup>/L，Hb：96g/L，Wbc：6.57×10<sup>9</sup>/L，中性粒比例：72.7%，Plt：222×10<sup>9</sup>/L。C反应蛋白：14.84MG/L，糖化血红蛋白：5.4%，低密度脂蛋白：1.93mmol/L，甘油三酯：0.81mmol/L，高密度脂蛋白：1.18mmol/L，总胆固醇：3.41mmol/L。Ca：2.27mmol/L，Cl：104.75mmol/L，K：4.03mmol/L，Na：142.27mmol/L，P：0.80mmol/L。白蛋白：36.93g/L，谷丙转氨酶：1.92u/L，间接胆红素：2.09umol/L，直接胆红素：5.48umol/L。肌酐：126.8umol/L，尿素氮：7.07mmol/L，尿酸：308.12umol/L，心电图：窦性心律，电轴左偏，T波改变，V2-V6低平。B超：肝脏轻度弥漫性病变，肝脏囊肿，肝内钙化灶。CT：右肺上叶前段部分实性结节，考虑恶性肿瘤可能，慢性支气管炎，双肺多发支气管扩张，两肺多发结节，前中纵膈内囊性病变，纵膈内多发淋巴结显示，部分钙化，主动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'587', N'1310', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-02-07 00:00:00.000', NULL, NULL, N'', NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'621', N'1344', N'患者家属', N'可信', N'王屹', N'2024-07-04 00:00:00.000', NULL, NULL, NULL, NULL, N'邹华，男，89岁，口齿不清、双下肢乏力四年余。患者四年前无明显诱因下突发双下肢乏力，伴口齿不清，经松江区中心医院诊治，诊断为：脑梗死，予抗血小板凝聚、稳定斑块等对症治疗，症状基本稳定。患者发病后理解力、记忆力、计算力明显减退，只能讲出自己姓名，双下肢行动缓慢，生活自理能力明显下降，今由家属送入本院住养。患者本次入院体检提示：贫血、肾功能不全、肺结节、右肾囊肿、异常Q波、心脏增大、动脉硬化等。患者目前精神状态可，纳可，睡眠较差，大小便较正常。左下肢骨折术后二年；高血压史十余年，目前无治疗。体检：体温 36.7℃，脉搏55次/分，呼吸18次/分，血压148/69mmHg。两肺呼吸音清，心率55次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。双下肢浮肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：5.33
mmol/L（松江区中心医院2024.7.1）。Rbc：3.73×10<sup>12</sup>/L，Hb：120g/L，Wbc：7.34×10<sup>9</sup>/L，中性粒比例：61.5%，淋巴细胞比例：26.3%，Plt：185
×10<sup>9</sup>/L（松江区中心医院2024.7.1）。低密度脂蛋白：4.13
mmol/L（松江区中心医院2024.7.1），甘油三酯：1.30mmol/L，高密度脂蛋白：1.17mmol/L，总胆固醇：5.68mmol/L。肌酐：131.68umol/L，尿素氮：8.08mmol/L，尿酸：422.59
umol/L（松江区中心医院2024.7.1），心电图：窦性心律，异常Q波(Ⅲ、aVF)
（松江区中心医院2024.7.1）B超：右肾囊肿
（松江区中心医院2024.7.1）CT：左侧基底节区腔隙灶，脑白质变性，老年脑改变；右肺下叶实性结节，心脏稍大，主动脉、冠状动脉硬化，少量心包积液
（松江区中心医院2024.7.1）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'622', N'1345', N'患者家属', N'可信', N'王屹', N'2024-07-08 00:00:00.000', NULL, NULL, NULL, NULL, N'何为民，男，100岁，时有头晕不适十余年。患者十余年前经常出现头晕头痛不适，经医院诊断为：高血压，予苯磺酸氨氯地平片治疗后血压基本稳定。患者平时服药欠规则，未定期监测血压。患者近期有数次跌倒史，家属只处理头部外伤，未进一步诊治。患者一年来口齿不清，认知功能下降明显，时有吵闹等异常行为，无就诊史。患者原有睡眠障碍6年余予艾司唑仑片治疗；原有前列腺增生5年余予盐酸坦索罗辛缓释胶囊等治疗；原有双足浮肿半年余予托拉塞米片等治疗。本次入院体检提示：慢性乙型病毒性肝炎予恩替卡韦片、当飞利肝宁胶囊治疗。另体检提示：脑梗死后遗症、冠心病、心律失常（频发房早）、心功能不全、贫血、肾功能不全、尿酸增高、右侧第11后肋骨折、胆囊息肉、右肾囊肿。患者目前精神状态尚可，生活自理能力明显减退，基本卧床，食欲、大小便较正常，睡眠欠佳。今由家属送至本院住养。左腹股沟疝术后十余年，前列腺增生术后五年体检：体温 36.3℃，脉搏72次/分，呼吸18次/分，血压138/62mmHg。口齿不清，问答欠切题，两肺呼吸音粗，心率72次/分，心律不齐，闻及频发早搏,无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，双下肢浮肿，左上肢肌力为4级,左下肢肌力为4级,右上肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：6.14
mmol/L（松江区中心医院2024.6.5）。Rbc：3.99×10<sup>12</sup>/L，Hb：108g/L，Wbc：4.95×10<sup>9</sup>/L，Plt：236
×10<sup>9</sup>/L（松江区中心医院2024.6.5）。糖化血红蛋白：5.9
%（松江区中心医院2024.6.5），低密度脂蛋白：3.84
mmol/L（松江区中心医院2024.6.5），总胆固醇：5.92mmol/L。谷丙转氨酶：8.05u/L，间接胆红素：4.15
umol/L（松江区中心医院2024.6.5），肌酐：109.73umol/L，尿素氮：7.42mmol/L，尿酸：506.71
umol/L（松江区中心医院2024.6.5），心电图：窦性心律，频发房性早搏
（松江区中心医院2024.6.5）B超：胆囊息肉，右肾小囊肿
（松江区中心医院2024.6.5）CT：双侧基底节、放射冠区散在腔隙灶，老年脑改变；（松江区中心医院2024.5.23）心脏增大，少量心包积液，少量胸腔积液；主动脉及冠状动脉硬化；右侧第11后肋骨折（松江区中心医院2024.6.6）
')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'625', N'1347', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-07-16 00:00:00.000', NULL, NULL, NULL, NULL, N'姜雪娟，女，105岁，反复胸闷、心悸10余年。患者10余年前无明显诱因下出现胸闷、心悸，无胸痛，无呼吸困难，无恶心呕吐，无意识障碍，于医院（具体不详）就诊，诊断“冠状动脉粥样硬化性心脏病”，予对症治疗（具体不详），病情好转后出院。因年事已高今入住我福利院。发病以来，患者无胸闷、胸痛，无心悸，无呕吐，无明显消瘦等，饮食睡眠可。 
    患者20余年前于上海市第六人民医院行右股骨颈骨折术（具体不详）体检：体温 36.7℃，脉搏71次/分，呼吸20次/分，血压154/73mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。71次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'628', N'1350', N'患者家属', N'基本可靠', N'胡新志', N'2024-08-07 00:00:00.000', NULL, NULL, N'', N'', N'苏凤仪，女，78岁，左侧肢体活动不利9年余。患者24年前无明显诱因出现反复头晕头痛，无恶心呕吐，诊断为“高血压”，长期口服硝苯地平片控释片，因服药不规律，血压控制不稳定，维持在160-180/80-100mmhg，未引起重视，于2016年7月突然意识不清，大小便失禁，家人发现后即送医院救治，诊断：“脑出血”，给予开颅手术治疗，(具体用药不详，诊疗经过不详)，手术治疗后病情好转出院，遗留左侧肢体活动不利。现因个人生活不能自理，于20240807自愿入院。入院体检提示：高脂血症，脂肪肝。患者一般情况尚可，轮椅推入病房，简单对答，口齿清晰，查体合作，饮食可，睡眠可，大便干燥，长期服用通便药物，小便正常。体检：体温 36.8℃，脉搏70次/分，呼吸18次/分，血压124/78mmHg。两肺呼吸音清，心率70次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为3级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。左手呈爪型，抓握不能。生化:葡萄糖：5.42mmol/L。糖化血红蛋白：6.0%，低密度脂蛋白：3.23mmol/L，甘油三酯：3.11mmol/L，高密度脂蛋白：1.05mmol/L，总胆固醇：5.20mmol/L。Ca：2.34mmol/L，Cl：102.63mmol/L，K：3.57mmol/L，Na：144.52mmol/L，P：1.10mmol/L。A/G：1.53，白蛋白：42.18g/L，谷丙转氨酶：26.05u/L，间接胆红素：6.12umol/L，碱性磷酸酶（ALP）：104.50u/L，总胆红素：9.83umol/L，总蛋白：69.75g/L。球蛋白：27.57g/L。直接胆红素：3.71umol/L。肌酐：67.68umol/L，尿素氮：5.08mmol/L，尿酸：479.62umol/L，白细胞：237u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：5.5，酮体：阴性（-），亚硝酸盐：++，隐血：+。心电图：窦性心律 异常Q波。B超：脂肪肝、肝脏囊肿，胆囊结石，右肾囊肿胰腺、脾脏、左肾未见明显占位，CT：胸部CT:心脏增大，主动脉及冠状动脉硬化。头颅CT：1.右额颞叶及右基底节区大片软化灶，脑室引流中2.老年脑改变3.右侧颅骨修补术后改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'634', N'1356', N'患者家属', N'基本可信', N'周佳明', N'2024-09-23 00:00:00.000', NULL, NULL, NULL, NULL, N'龚瑞昌，男，73岁，反复头晕4年余。患者4年前无明显诱因下出现反复头晕，无明显头痛，无视物旋转，无恶心呕吐，无肢体活动障碍，曾至松江中心医院就诊，诊断为“高血压”长期口服苯磺酸氨氯地平片降压，血压控制尚稳定。4年前体检发现血糖、血脂升高，诊断：“糖尿病2型、高脂血症”，长期服用西格列汀片降糖、普伐他汀钠片降脂，自诉血糖、血脂控制尚可。3年前配偶去世后出现近记忆力下降，认知功能减退，松江中心医院诊断：“阿尔茨海默病”长期口服重酒石酸卡巴拉汀胶囊及甲磺酸双氢麦角毒碱片对症治疗。现因年事已高、个人生活不能自理，于20240923自愿入住我院。入院体检提示：左肺下叶基底段实性结节；老年脑改变。患者一般情况尚可，步入病室，反应可，对答切题，查体合作，饮食可，睡眠尚可，大小便正常，双下肢无浮肿。否认肝炎、结核等传染病史。体检：体温 36.4℃，脉搏108次/分，呼吸19次/分，血压147/88mmHg。两肺呼吸音清。108次/分，心律齐。无腹壁紧张，无压痛，无反跳痛。四肢无水肿,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:Rbc：4.54×10<sup>12</sup>/L，Hb：153.00g/L，Wbc：8.54×10<sup>9</sup>/L，中性粒比例：78%，淋巴细胞比例：18.1%，Plt：275×10<sup>9</sup>/L。糖化血红蛋白：6.1%，Cl：99.52mmol/L，K：4.08mmol/L，Na：141.12mmol/L，A/G：1.79，白蛋白：49.47g/L，谷丙转氨酶：34.13u/L，间接胆红素：11.18umol/L，碱性磷酸酶（ALP）：81.78u/L，总胆红素：17.56umol/L，总蛋白：77.07g/L。球蛋白：27.6g/L。直接胆红素：6.38umol/L。肌酐：103.21umol/L，尿素氮：4.59mmol/L，尿酸：297.48umol/L，心电图：窦性心律 T波改变。B超：1.脂肪肝；2.双肾结石。CT：头颅CT提示：双侧基底节、放射冠区散在腔隙灶，脑白质变性，老年脑改变。胸部CT提示：1.左肺下叶外基底段实性结节；2.双肺肺气肿；3.主动脉及冠状动脉硬化。（20240628松江医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'638', N'1360', N'患者家属', N'基本可信', N'周佳明', N'2024-10-21 00:00:00.000', NULL, NULL, NULL, NULL, N'钱金珍，女，82岁，记忆力进行性减退1年。家属代诉患者1年前开始出现记忆力进行性减退，丢三落四、易忘事，总觉得自己东西被偷，时有藏东西行为，偶有不认识家人，无猜疑、被害妄想。情绪较为稳定，无明显吵闹、纠缠不清。曾至松江精神卫生中心就诊，诊断为“阿尔茨海默症（混合型）”。目前予以抗精神疾病药物干预。患者原有高血压病史20年余，最高血压（家属诉不详），未口服药物，自诉血压控制可。患者原有冠心病病史10余年，长期服药，自诉病情控制尚可，无胸闷气促等症状。患者原有慢性肾脏病、肾性贫血2年余，长期服药，自诉病情控制可。患者原有白细胞减少症1年余，长期服药，自诉病情尚可。患者原有高尿酸血症1年，目前未服药，自诉病情控制尚可。因年事已高、个人生活不能自理于20241021自愿入住我院。患者一般情况尚可，步入病房，简单对答，听力功能正常，查体合作，饮食可、睡眠可，大小便正常。否认曾有慢性支气管炎，糖尿病病等其他慢性病史 体检：体温 37℃，脉搏68次/分，呼吸18次/分，血压145/79mmHg。两肺呼吸音清，68次/分，心律齐，未闻及早搏。无腹壁紧张，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.61mmol/LRbc：3.86×10<sup>12</sup>/L，Hb：124g/L，Wbc：3.09×10<sup>9</sup>/L，中性粒比例：72.1%，淋巴细胞比例：22%，Plt：139×10<sup>9</sup>/L。糖化血红蛋白：5.4%，低密度脂蛋白：0.85mmol/L，甘油三酯：0.54mmol/L，高密度脂蛋白：1.36mmol/L，总胆固醇：2.31mmol/L。Ca：2.22mmol/L，Cl：107.5mmol/L，K：4.41mmol/L，Na：142.6mmol/L，P：1.02mmol/L。总胆红素：25.05umol/L，总蛋白：68.57g/L。球蛋白：27.87g/L。直接胆红素：10.46umol/L。肌酐：86.4umol/L，尿素氮：7.74mmol/L，尿酸：362.6umol/L，白细胞：6-8u/L，亚硝酸盐：-，隐血：1+。心电图：1.窦性心动过缓；2.T波变化V4~V6低平。X线胸片：(缺)。B超：双肾多发液性占位--囊肿可能（20241016第五康复医院）CT：1.老年脑改变；2.右肺上叶实性结节，右肺下叶磨玻璃样结节；3.左肺门继纵膈内部分淋巴结改变；4.心脏增大，少量心包积液。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'641', N'1363', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-01-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'644', N'1366', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-02-24 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'647', N'1369', N'患者家属', N'基本可靠', N'涂宝玲', N'2025-03-13 00:00:00.000', NULL, NULL, N'', N'', N'朱慧珠，女，82岁，反复头晕头痛40余年。患者40余年前无诱因下出现头晕头痛，无恶心呕吐，无意识障碍，于上海市第一人民医院就诊，最高血压200/100mmHg，诊断“高血压病”，予以奥美沙坦酯片、非洛地平缓释片降压治疗后症状有所缓解，目前血压稳定，维持在120-140/70-90mmHg之间。发病以来，患者无发热，无呕吐，无明显消瘦等，饮食睡眠可。
患者因高血压病III级 极高危、冠心病（支架置入术后）、帕金森病、慢性肾功能不全、焦虑抑郁状态、便秘等目前予以奥氮平片、硫酸氢氯吡格雷片、奥美沙坦酯片、多巴丝肼片、复方α-酮酸片、乳果糖口服治疗。患者因高血压病40余年、帕金森病38年、冠心病（支架术后）15年、高脂血症15年、右侧股骨骨折术后3月、骨质疏松、高尿酸血症、慢性肾功能不全、慢性胃炎、焦虑状态、心律失常、双眼黄斑变性（光感）体检：体温 36.5℃，脉搏78次/分，呼吸20次/分，血压118/62mmHg。两肺呼吸音清，心率78次/分，心律齐，腹软，无压痛，生化:葡萄糖：5.0mmol/L，Rbc：2.67×10<sup>12</sup>/L，Hb：86g/L，Wbc：5.89×10<sup>9</sup>/L，中性粒比例：49.2%，淋巴细胞比例：37.9%，Plt：227×10<sup>9</sup>/L，糖化血红蛋白：6%，低密度脂蛋白：1.91mmol/L，甘油三酯：1.38mmol/L，高密度脂蛋白：0.78mmol/L，总胆固醇：2.97mmol/L，Cl：105.4mmol/L，K：4.2mmol/L，Na：140.3mmol/L，A/G：1.6，白蛋白：35.8g/L，谷丙转氨酶：3u/L，碱性磷酸酶（ALP）：101u/L，总胆红素：7.5umol/L，总蛋白：58.6g/L。球蛋白：22.8g/L。直接胆红素：1.3umol/L，肌酐：109.5umol/L，尿素氮：9.7mmol/L，尿酸：449.48umol/L。心电图：窦性心律，不完全性右束支传导阻滞，T波改变；X线胸片：左髋关节退变，左侧股骨颈上方致密影，右髋关节置换术后，右髋退行性改变，右髋、股骨上端周围软组织内高密度影，两膝退行性改变，右侧髂骨骨岛可能；B超：双肾囊肿，双侧输尿管、膀胱未见明显异常。肝囊肿，胆囊内充满结石可能，双肾囊肿，胰腺、脾脏未见明显异常；CT：老年脑改变，脑白质病。右肺微小结节，右肺下叶间质性改变，两肺多发条索影，心脏增大，主动脉及冠状动脉管壁钙化右乳点状钙化影，两侧多发肋骨骨折，肝脏、右肾低密度灶，胆囊多发结石，胆总管扩张（2025.2.27上海市养志康复医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'660', N'1382', N'患者本人及家属', N'基本可信', N'周佳明', N'2025-05-09 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'423', N'1146', N'患者家属', N'可信', N'袁纯兰', N'2022-02-23 00:00:00.000', NULL, NULL, NULL, NULL, N'石元妹，女，73岁，记忆力减退七年余。患者于约八年前开始出现记忆力减退，易忘事，不记得家庭住址，不记得家人名字，日常生活能力下降。2015年12月因记忆力下降伴吵闹，言语不能表达在松江区精神卫生中心住院治疗，诊断“阿尔茨海默病性痴呆”，给予盐酸美金刚、富马酸喹硫平片、氢溴酸西酞普兰、米氮平、劳拉西泮等改善情绪及精神行为症状，后趋于稳定于2016年2月出院。出院后定期外配盐酸美金刚口服，病情控制可。2020年起停服美金刚。现因患者生活不能自理入住我院。发病以来无明显消瘦，饮食可，夜眠尚可，大小便正常。否认肝炎、结核病史。无高血压、糖尿病、冠心病等病史。既往便秘病史，服用便通胶囊。体检：体温 36.1℃，脉搏76次/分，呼吸20次/分，血压111/70mmHg。两肺呼吸音清，无心率72次/分，心律齐，腹软，无压痛，四肢无水肿，肌张力增强，左上肢肌力为4级,左下肢肌力为4级,生化:葡萄糖：8.14mmol/L，中性粒比例：83.1%，淋巴细胞比例：11.5%，甘油三酯：1.57mmol/L，高密度脂蛋白：1.52mmol/L，总胆固醇：6.43mmol/L。Ca：2.27mmol/L，Cl：105.31mmol/L，K：3.82mmol/L，Na：141.08mmol/L，P：1.26mmol/L，心电图：窦性心律，st改变，T波改变，B超：胆囊结石，CT：右肺中叶实性结节，慢性支气管炎，左肺上叶段部分肺不张，心脏稍大，主动脉及冠状动脉硬化，甲状腺左叶结节；右侧放射冠区腔隙灶，老年脑改变（2022.2.22松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'428', N'1152', N'患者本人及家属', N'可信', N'庄秋丽', N'2022-07-05 00:00:00.000', NULL, NULL, NULL, NULL, N'高花朵，女，93岁，患者反复发作性头晕不适20余年。患者原有高血压病20余年，反复出现头晕不适，最初因头晕不适于医院就诊时发现高血压，以往最高血压180/65mmHg，平时长期规律服用苯磺酸氨氯地平片，据说目前血压控制可。
患者曾有脑动脉供血不足病史20余年，反复头晕无力不适，长期服用丹参及银杏叶片活血通络。
患者两下肢时常酸痛不适20余年，近阶段曾多次于松江中心医院CT检查：两膝关节退行性改变。右下肢行走稍拐，据说20年前曾被车撞，当时未就诊检查。
原有血吸虫肝病史60余年，高血压病20余年，脑动脉供血不足20余年，两下肢时常酸痛不适20余年。
体检：体温 36.7℃，脉搏68次/分，呼吸20次/分，血压119/52mmHg。神清，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率68次/分，心律齐，腹平软，全腹无压痛，肝脾肋下未及肿大，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。
生化:葡萄糖：5.04mmol/L。Rbc：2.86×10<sup>12</sup>/L，Hb：87g/L，Wbc：3.93×10<sup>9</sup>/L，中性粒比例：60.60%，淋巴细胞比例：29.80%，Plt：137×10<sup>9</sup>/L。糖化血红蛋白：5.10%，低密度脂蛋白：3.14mmol/L，甘油三酯：1.10mmol/L，高密度脂蛋白：2.25mmol/L，总胆固醇：5.43mmol/L。Ca：2.42mmol/L，Cl：101.06mmol/L，K：4.21mmol/L，Na：139.36mmol/L，P：1.41mmol/L。A/G：1.46，白蛋白：42.86g/L，谷丙转氨酶：5.97u/L，间接胆红素：6.35umol/L，碱性磷酸酶（ALP）：50.54u/L，总胆红素：9.43umol/L，总蛋白：72.15g/L。球蛋白：29.29g/L。直接胆红素：3.08umol/L。肌酐：73.62umol/L，尿素氮：7.83mmol/L，尿酸：440.64umol/L，白细胞：5.00u/L，管型：0.00u/L，尿胆原：阴性，葡萄糖：阴性mmol/L，酸碱度：6.5，酮体：阴性，上皮细胞：1.00u/L，亚硝酸盐：阴性，隐血：阴性。心电图：窦性心律，左室高电压。X线胸片：(缺)。B超：血吸虫肝病，肝脏囊肿，胰腺主胰管显示，双肾囊肿，胆囊、脾脏未见明显占位。CT：头颅：双侧放射性冠区腔隙灶，老年脑改变。胸部：右肺中叶磨玻璃结节，较前相仿，慢性支气管炎改变。心脏增大，主动脉及冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'432', N'1156', N'患者家属', N'基本可信', N'周婷', N'2022-08-18 00:00:00.000', NULL, NULL, NULL, NULL, N'金静雯，女，60岁，时有头晕头痛十余年患者十余年之前无明显诱因下时常出现间断性头晕、头痛伴视物模糊，无胸闷胸痛，无恶心呕吐等不适症状，外院就诊后诊断为高血压病，长期服用氯沙坦钾片等降压药物治疗，血压控制在120/80mmaHg,病情较稳定。目前因患者年事渐高，从小又患有小儿麻痹症，家中无人照顾，于2022年8月18号入住本院。患者发病以来精神、饮食、睡眠均可，大小便正常。体检：体温 36.4℃，脉搏91次/分，呼吸20次/分，血压146/86mmHg。两肺呼吸音粗，无干啰音。心率91次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，有肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：5.46  mmol/L（松江区中心医院）。Rbc：3.7 ×10<sup>12</sup>/L（松江区中心医院），Hb：119g/L，Wbc：4.52×10<sup>9</sup>/L，中性粒比例：49.4%，淋巴细胞比例：37.4%，Plt：230×10<sup>9</sup>/L。C反应蛋白：未检测MG/L，糖化血红蛋白：5.7  %（松江区中心医院），低密度脂蛋白：3.23mmol/L，甘油三酯：1.39  mmol/L（松江区中心医院），高密度脂蛋白：1.54mmol/L，总胆固醇：5.02mmol/L。Ca：2.39mmol/L，Cl：104.87mmol/L，K：4.92   mmol/L（松江区中心医院），Na：141.22mmol/L，P：1.25mmol/L。A/G：1.5，白蛋白：43.22g/L，谷丙转氨酶：19.83  u/L（松江区中心医院），间接胆红素：7.43umol/L，碱性磷酸酶（ALP）：124.55u/L，总胆红素：10.43umol/L，总蛋白：71.97g/L。球蛋白：28.75g/L。直接胆红素：3.00umol/L。肌酐：80.12umol/L，尿素氮：5.57   mmol/L（松江区中心医院），尿酸：284.32umol/L，白细胞：19u/L，管型：0.00u/L，尿胆原：-   （松江区中心医院），葡萄糖：-mmol/L，酸碱度：5.0，酮体：-，上皮细胞：2u/L，亚硝酸盐：-，隐血：+。心电图：正常心电图X线胸片：(缺)。B超：肝、胆、胰、脾、双肾未见异常CT：1.左肺上叶磨玻璃结节 2.主动脉硬化 3.肝脏散在囊性灶 4.老年脑改变')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'444', N'1166', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政志，男，70岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕等可以独立完成，洗浴需要辅助。患者目前服用吲达帕胺片控制血压。本次入院前体检提示胆囊炎、高血压病。既往急性胰腺炎病史数年。体检：体温 36.8℃，脉搏102次/分，呼吸20次/分，血压138/79mmHg。两肺呼吸音粗，心率02次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：4.58mmol/L。Rbc：4.88×10<sup>12</sup>/L，Hb：150g/L，Wbc：6×10<sup>9</sup>/L，中性粒比例：32.8%，淋巴细胞比例：63.9%，Plt：129×10<sup>9</sup>/L。低密度脂蛋白：1.12mmol/L（2022.11.15上海市上农医院），甘油三酯：0.86mmol/L，高密度脂蛋白：1.33mmol/L，总胆固醇：3.24mmol/L。Cl：104.1mmol/L，K：3.97mmol/L，Na：144.3mmol/L，A/G：1.69，白蛋白：41.3g/L，谷丙转氨酶：18u/L，间接胆红素：13.1umol/L，碱性磷酸酶（ALP）：75u/L，总胆红素：14.7umol/L，总蛋白：65.7g/L。球蛋白：24.4g/L。直接胆红素：1.6umol/L。肌酐：78umol/L，尿素氮：7mmol/L，尿酸：276umol/L（2022.11.15上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'427', N'1151', N'患者本人及家属', N'基本可靠', N'胡新志', N'2022-06-14 00:00:00.000', NULL, NULL, N'', N'', N'王雪娟，女，81岁，左大腿骨折术后日常生活依赖8月。患者于2021年9月29日夜间小便时不慎跌倒致左下肢活动障碍，在第六人民医院诊断“左股骨干骨折”，予以手术治疗，术后在上海养志康复医院进行康复锻炼3个月，左髋关节主动活动尚可，左下肢肌力4级，在监护下完成穿脱裤子，借助助步器可持续行走15分钟。目前因年事已高，个人生活不能自理，于20220614自愿入住我院。体检：体温 37.1℃，脉搏67次/分，呼吸20次/分，血压130/73mmHg。两肺呼吸音清，心率67次/分，律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：4.4mmol/L（2022-6-14上海养志康复医院）。Rbc：4.36×10<sup>12</sup>/L，Hb：126g/L，Wbc：5.3×10<sup>9</sup>/L，中性粒比例：44.4%，淋巴细胞比例：44.4%，Plt：261×10<sup>9</sup>/L（2022-6-14上海养志康复医院）。糖化血红蛋白：6.2%（2022-6-14上海养志康复医院），低密度脂蛋白：3.91mmol/L（2022-6-14上海养志康复医院），甘油三酯：1.67mmol/L，高密度脂蛋白：1.46mmol/L，总胆固醇：6.12mmol/L。Ca：1.18mmol/L（2022-6-14上海养志康复医院），Cl：105.7mmol/L，K：3.19mmol/L，Na：143.7mmol/L，A/G：1.23，白蛋白：42.2g/L，谷丙转氨酶：10u/L，碱性磷酸酶（ALP）：66u/L（2022-6-14上海养志康复医院），总胆红素：15.9umol/L，总蛋白：76.5g/L。球蛋白：34.3g/L。直接胆红素：3.8umol/L。肌酐：43umol/L，尿素氮：3.1mmol/L，尿酸：235.4umol/L（2022-6-14上海养志康复医院），心电图：(缺)。X线胸片：(缺)。B超：(缺)。CT：胸部CT：1.慢性支气管炎2.纵膈内淋巴结显示3.心影明显增大 主动脉及冠状动脉管壁钙化4.右后胸膜略增厚5.左乳结节 附见：肝脏内多发钙化灶。（2022-6-14上海养志康复医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'457', N'1185', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦兰，女，69岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、行走等可以独立完成，穿脱衣服、洗浴、如厕需要辅助。本次入院体检提示胆囊未显示、血脂偏高。体检：体温 36.1℃，脉搏74次/分，呼吸20次/分，血压144/86mmHg。两肺呼吸音粗，心率74次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿，肌力正常。生化:葡萄糖：4.46mmol/L。低密度脂蛋白：3.21mmol/L，甘油三酯：0.68mmol/L，高密度脂蛋白：2.08mmol/L，总胆固醇：5.89mmol/L。谷丙转氨酶：13u/L，碱性磷酸酶（ALP）：80u/L，总胆红素：19.9umol/L，肌酐：67umol/L，尿素氮：5.08mmol/L，尿酸：178umol/L。心电图：窦性心律，右室传导延迟。B超：胆囊未显示（2022.10.10上海市上农医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'458', N'1193', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵梦燕，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似聋哑，日常生活中洗漱、洗浴、进食、穿脱衣服、行走、如厕等可以独立完成，本次入院体检提示窦性心动过缓，血糖偏高。体检：体温 36.7℃，脉搏72次/分，呼吸20次/分，血压116/80mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率72次/分，心律齐，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：7.03mmol/L（2022.10.10上海市上农医院）。Rbc：4.11×10<sup>12</sup>/L，Hb：123g/L，Wbc：5.45×10<sup>9</sup>/L，中性粒比例：56.8%，淋巴细胞比例：38%，Plt：232×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.54mmol/L，甘油三酯：0.62mmol/L，高密度脂蛋白：1.58mmol/L，总胆固醇：6.54mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：89u/L，总胆红素：10.8umol/L（2022.10.10上海市上农医院），肌酐：61umol/L，尿素氮：5.67mmol/L，尿酸：252umol/L（2022.10.10上海市上农医院），心电图：窦性心动过缓（2022.10.10上海市上农医院）X线胸片：（-）（2022.10.10上海市上农医院）B超：胆囊未显示（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'460', N'1167', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政建，男，79岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、洗浴、如厕等可以独立完成。本次入院体检提示高血压病、高脂血症。体检：体温 36.5℃，脉搏95次/分，呼吸20次/分，血压144/86mmHg。两肺呼吸音清，无干啰音、湿啰音，心率95次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.6mmol/L（2022.10.10上海市上农医院）。Rbc：5.37×10<sup>12</sup>/L，Hb：152g/L，Wbc：5.51×10<sup>9</sup>/L，中性粒比例：55.5%，淋巴细胞比例：33.8%，Plt：275×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：4.88mmol/L，甘油三酯：0.95mmol/L，高密度脂蛋白：1.88mmol/L，总胆固醇：7.11mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：24u/L，碱性磷酸酶（ALP）：102u/L，总胆红素：16.6umol/L（2022.10.10上海市上农医院），肌酐：74umol/L，尿素氮：7.09mmol/L，尿酸：347umol/L（2022.10.10上海市上农医院），心电图：窦性心律，左室高电压（2022.10.10上海市上农医院）X线胸片：心影增大（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'461', N'1197', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政文，男，66岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，疑似听力障碍，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示窦性心动过缓，I度房室传导阻滞。体检：体温 37.1℃，脉搏78次/分，呼吸20次/分，血压138/83mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率78次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.91mmol/L（2022.10.10上海市上农医院）。Rbc：4.91×10<sup>12</sup>/L，Hb：137g/L，Wbc：6.85×10<sup>9</sup>/L，中性粒比例：52.9%，淋巴细胞比例：35%，Plt：176×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：1.35mmol/L，甘油三酯：0.62mmol/L，高密度脂蛋白：2.16mmol/L，总胆固醇：4.18mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：19u/L，碱性磷酸酶（ALP）：95u/L，总胆红素：10.3umol/L（2022.10.10上海市上农医院），肌酐：74umol/L，尿素氮：5.91mmol/L，尿酸：174umol/L（2022.10.10上海市上农医院），心电图：窦性心动过缓，I度房室传导阻滞（2022.10.10上海市上农医院）X线胸片：左上肺陈旧性肺结核（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'467', N'1175', N'其他', N'基本可靠', N'周婷', N'2022-11-30 00:00:00.000', NULL, NULL, N'救助二站', N'', N'辽珍珍，女，66岁，智力低下几十年患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下。日常生活中可独立完成洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高；尿酸偏高。心电图检查：窦性心律、T波低平；B超提示：脂肪肝。患者目前精神状态可，食欲、大小便、睡眠较正常。另患者因高血压在服吲达帕胺片，血压控制较稳定。体检：体温 37℃，脉搏90次/分，呼吸20次/分，血压122/87mmHg。两肺呼吸音清，无干啰音、无哮鸣音。心率90次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.02  mmol/L（上海市上农医院）。Rbc：4.99×10<sup>12</sup>/L（上海市上农医院），Hb：143g/L，Wbc：7.77×10<sup>9</sup>/L，中性粒比例：63.1%，淋巴细胞比例：29.4%，Plt：307×10<sup>9</sup>/L。低密度脂蛋白：3.99mmol/L，甘油三酯：1.53 mmol/L（上海市上农医院），高密度脂蛋白：1.71mmol/L，总胆固醇：6.4mmol/L。谷丙转氨酶：18  u/L（上海市上农医院），碱性磷酸酶（ALP）：63u/L，总胆红素：6.0umol/L，肌酐：77umol/L，尿素氮：7.81 mmol/L（上海市上农医院），尿酸：399umol/L，心电图：窦性心律、T波低平X线胸片：（-）B超：脂肪肝。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'469', N'1181', N'其他', N'基本可靠', N'张丁', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'王志超，女，72岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、洗浴、行走、如厕等方面能够独立完成。入院前体检提示左室高电压，总胆固醇、低密度脂蛋白胆固醇偏高。有糖尿病病史，目前二甲双胍缓释片500mg Qd口服治疗中。体检：体温 36.5℃，脉搏67次/分，呼吸17次/分，血压135/82mmHg。两肺呼吸音粗，无干湿啰音。心率80次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.60mmol/L（南京艾迪康医学检验所，2022-10-10）。Rbc：3.91×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-10），Hb：125g/L，Wbc：8.05×10<sup>9</sup>/L，中性粒比例：68.5%，淋巴细胞比例：24.2%，Plt：317×10<sup>9</sup>/L。低密度脂蛋白：7.19mmol/L，甘油三酯：0.44mmol/L（南京艾迪康医学检验所，2022-10-10），高密度脂蛋白：2.64mmol/L，总胆固醇：10.71mmol/L。谷丙转氨酶：10u/L（南京艾迪康医学检验所，2022-10-10），碱性磷酸酶（ALP）：104u/L，总胆红素：8.8umol/L，肌酐：68umol/L，尿素氮：7.41mmol/L（南京艾迪康医学检验所，2022-10-10），尿酸：238umol/L，心电图：窦性心律，左室高电压。（上海市上农医院，2022-10-10）X线胸片：正常（上海市上农医院，2022-10-10）B超：正常（上海市上农医院，2022-10-10）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'472', N'1256', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民本，男，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上洗脸刷牙、进食、穿脱衣、洗浴、行走、如厕等方面能够独立完成。入院前体检提示窦性心动过缓，肝功能异常，血小板偏低，HBsAg阳性，HBeAb阳性，HBcAb阳性。无糖尿病史。有乙型肝炎史。体检：体温36.8℃，脉搏90次/分，呼吸20次/分，血压133/88mmHg。两肺呼吸音清，无干湿啰音。心率90次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：3.72mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.91×10<sup>12</sup>/L（南京艾迪康医学检验所，2022-10-12），Hb：165g/L，Wbc：4.99×10<sup>9</sup>/L，中性粒比例：74.3%，淋巴细胞比例：20.6%，Plt：168×10<sup>9</sup>/L。低密度脂蛋白：3.07mmol/L，甘油三酯：0.98mmol/L（南京艾迪康医学检验所，2022-10-12），高密度脂蛋白：1.27mmol/L，总胆固醇：4.56mmol/L。谷丙转氨酶：20u/L（南京艾迪康医学检验所，2022-10-12），碱性磷酸酶（ALP）：70u/L，总胆红素：40.1umol/L，肌酐：69umol/L，尿素氮：5.13mmol/L（南京艾迪康医学检验所，2022-10-12），尿酸：228umol/L，心电图：窦性心动过缓。（上海市上农医院，2022-10-12）X线胸片：正常。（上海市上农医院，2022-10-12）B超：正常。（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'474', N'1257', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'廖广发，男，78岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗浴、如厕等方面能够独立完成。该患者有高血压病史，长期口服硝苯地平片治疗。体检：体温 36.7℃，脉搏88次/分，呼吸20次/分，血压102/76mmHg。两肺呼吸音清，无干湿啰音。心率88次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.31mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.64×10<sup>12</sup>/L，Hb：138g/L，Wbc：5.10×10<sup>9</sup>/L，中性粒比例：70.7%，淋巴细胞比例：19.0%，Plt：202×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：2.73mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：0.95mmol/L，高密度脂蛋白：1.37mmol/L，总胆固醇：4.33mmol/L。谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：87u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：8.0umol/L，肌酐：66umol/L，尿素氮：8.86mmol/L，尿酸：283umol/L（南京艾迪康医学检验所，2022-10-12），心电图：正常（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'478', N'1214', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民安，男，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。体检：体温 37.1℃，脉搏73次/分，呼吸20次/分，血压133/76mmHg。两肺呼吸音清，无干湿啰音。心率73次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.21mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：5.18×10<sup>12</sup>/L，Hb：166g/L，Wbc：7.57×10<sup>9</sup>/L，中性粒比例：53.2%，淋巴细胞比例：40.0%，Plt：184×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：5.10mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：2.03mmol/L，高密度脂蛋白：1.79mmol/L，总胆固醇：7.42mmol/L。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：85u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：10.6umol/L，肌酐：68umol/L，尿素氮：6.51mmol/L，尿酸：415umol/L（南京艾迪康医学检验所，2022-10-12），心电图：窦性心律，ST-T段改变。（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：脂肪肝（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'479', N'1215', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民成，男，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面基本自理。患者目前口服利培酮治疗中。体检：体温 37.4℃，脉搏107次/分，呼吸20次/分，血压126/88mmHg。两肺呼吸音清，无干湿啰音。心率107次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿。生化:葡萄糖：5.71mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：4.78×10<sup>12</sup>/L，Hb：147g/L，Wbc：5.57×10<sup>9</sup>/L，中性粒比例：50.9%，淋巴细胞比例：42.6%，Plt：197×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：2.45mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：1.2mmol/L，高密度脂蛋白：1.14mmol/L，总胆固醇：3.92mmol/L。谷丙转氨酶：33u/L，碱性磷酸酶（ALP）：80u/L，总胆红素：7.0umol/L，肌酐：68umol/L，尿素氮：5.73mmol/L，尿酸：322umol/L（南京艾迪康医学检验所，2022-10-12），心电图：窦性心律，ST段改变。（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'480', N'1249', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'473', N'1205', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'483', N'1209', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋民根，男，68岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。入院前体检提示尿酸、血脂偏高。患者因高血压病目前口服硝苯地平中。体检：体温 36.4℃，脉搏86次/分，呼吸20次/分，血压134/84mmHg。两肺呼吸音清，无干湿啰音。心率86次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.86mmol/L（南京艾迪康医学检验所，2022-10-12）。Rbc：5.56×10<sup>12</sup>/L，Hb：176g/L，Wbc：6.80×10<sup>9</sup>/L，中性粒比例：58.8%，淋巴细胞比例：30.9%，Plt：160×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：4.23mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：1.21mmol/L，高密度脂蛋白：1.25mmol/L，总胆固醇：5.58mmol/L。谷丙转氨酶：28u/L，碱性磷酸酶（ALP）：17u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：15.2umol/L，肌酐：120umol/L，尿素氮：6.49mmol/L，尿酸：471umol/L（南京艾迪康医学检验所，2022-10-12），心电图：窦性心律，T波改变（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'486', N'1198', N'其他', N'基本可靠', N'袁纯兰', N'2022-11-30 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'赵政祥，男，68岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴需要辅助。本次入院体检提示支气管扩张症、陈旧性肺结核、肝血管瘤、乙型病毒性肝炎等。 体检：体温 36.4℃，脉搏90次/分，呼吸20次/分，血压122/81mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率90次/分，心律齐，发热，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：4.88mmol/L（2022.10.10上海市上农医院）。Rbc：4.91×10<sup>12</sup>/L，Hb：150g/L，Wbc：9.22×10<sup>9</sup>/L，中性粒比例：85.4%，淋巴细胞比例：9%，Plt：269×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.04mmol/L，甘油三酯：0.74mmol/L，高密度脂蛋白：1.23mmol/L，总胆固醇：3.71mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：21u/L，碱性磷酸酶（ALP）：67u/L，总胆红素：11.4umol/L（2022.10.10上海市上农医院），肌酐：64umol/L，尿素氮：5.4mmol/L，尿酸：160umol/L（2022.10.10上海市上农医院），心电图：正常心电图（2022.10.10上海市上农医院）X线胸片：两肺陈旧性肺结核（2022.10.10上海市上农医院）B超：肝血管瘤（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'492', N'1243', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'494', N'1235', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'李世英，女，63岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在生活上进食、穿衣、洗澡、如厕等方面能够独立完成。体检：体温 36.3℃，脉搏72次/分，呼吸18次/分，血压148/72mmHg。两肺呼吸音清，无干湿啰音。心率72次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.25mmol/L。Rbc：4.35×10<sup>12</sup>/L，Hb：129g/L，Wbc：6.45×10<sup>9</sup>/L，中性粒比例：53.6%，淋巴细胞比例：37.1%，Plt：269×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-12）。低密度脂蛋白：3.10mmol/L（南京艾迪康医学检验所，2022-10-12），甘油三酯：0.82mmol/L，高密度脂蛋白：1.65mmol/L，总胆固醇：5.46mmol/L。谷丙转氨酶：16u/L，碱性磷酸酶（ALP）：89u/L（南京艾迪康医学检验所，2022-10-12），总胆红素：5.46umol/L，肌酐：66umol/L，尿素氮：6.67mmol/L，尿酸：227umol/L（南京艾迪康医学检验所，2022-10-12），心电图：大致正常（上海市上农医院，2022-10-12）X线胸片：正常（上海市上农医院，2022-10-12）B超：正常（上海市上农医院，2022-10-12）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'495', N'1221', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆亚，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示血脂偏高。体检：体温 36.6℃，脉搏73次/分，呼吸20次/分，血压123/73mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率73次/分，心律齐，腹软，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：4.63mmol/L（2022.10.10上海市上农医院）。Rbc：4.31×10<sup>12</sup>/L，Hb：129g/L，Wbc：5.43×10<sup>9</sup>/L，中性粒比例：54.5%，淋巴细胞比例：40.2%，Plt：268×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：3.51mmol/L，甘油三酯：1.04mmol/L，高密度脂蛋白：1.91mmol/L，总胆固醇：6.01mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：15u/L，碱性磷酸酶（ALP）：101u/L，总胆红素：9umol/L（2022.10.10上海市上农医院），肌酐：55umol/L，尿素氮：6.12mmol/L，尿酸：163umol/L（2022.10.10上海市上农医院），心电图：窦性心律轻度左室电压增高（2022.10.10上海市上农医院）X线胸片：（-）（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）CT：梅毒特异性抗体（-）丙肝抗体（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'500', N'1234', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆芝0901，女，69岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣、洗漱、进食、行走、如厕等能完全自理。有高血压史长期服用复方卡托普利片。入院体检提示：高血脂症、高尿酸血症、肌酐偏高。患者目前精神状态可，食欲、大小便、睡眠较正常。 体检：体温 36.9℃，脉搏73次/分，呼吸20次/分，血压108/73mmHg。两肺呼吸音清，73次/分，心律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.79mmol/L（2022-10-14上海市上农医院）。Rbc：4.48×10<sup>12</sup>/L，Hb：133g/L，Wbc：6.94×10<sup>9</sup>/L，中性粒比例：64.2%，淋巴细胞比例：30.0%，Plt：267×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：4.24mmol/L（2022-10-14上海市上农医院），甘油三酯：1.78mmol/L，高密度脂蛋白：1.59mmol/L，总胆固醇：6.78mmol/L。谷丙转氨酶：40u/L，碱性磷酸酶（ALP）：101u/L（2022-10-14上海市上农医院），总胆红素：10.1umol/L，肌酐：83umol/L，尿素氮：8.40mmol/L，尿酸：395umol/L（2022-10-14上海市上农医院），心电图：正常心电图（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'502', N'1219', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆蓉0858，女，69岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣、洗漱、进食、行走、如厕等能完全自理，能简单交流。入院体检提示：高胆固醇血症、心肌供血不足。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核等传染病史。体检：体温 36.5℃，脉搏80次/分，呼吸20次/分，血压103/68mmHg。两肺呼吸音清，80次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.43mmol/L（2022-10-14上海市上农医院）。Rbc：4.14×10<sup>12</sup>/L，Hb：129g/L，Wbc：4.31×10<sup>9</sup>/L，中性粒比例：62%，淋巴细胞比例：31.3%，Plt：186×10<sup>9</sup>/L。低密度脂蛋白：2.86mmol/L（2022-10-14上海市上农医院），甘油三酯：0.98mmol/L，高密度脂蛋白：1.93mmol/L，总胆固醇：5.54mmol/L。谷丙转氨酶：23u/L，碱性磷酸酶（ALP）：65u/L（2022-10-14上海市上农医院），总胆红素：16.7umol/L，肌酐：74umol/L，尿素氮：5.52mmol/L，尿酸：236umol/L（2022-10-14上海市上农医院），心电图：窦性心律 T波低平（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'508', N'1225', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆慧0870，女，75岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗浴、穿脱衣、洗漱、进食、行走、如厕等可独立完成。入院体检提示：脂肪肝、高脂血症、心肌供血不足。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核史，有胆囊炎、梅毒史。体检：体温 36.7℃，脉搏105次/分，呼吸20次/分，血压120/89mmHg。两肺呼吸音清，105次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。右小腿前侧皮肤片状皮疹、色素沉着。生化:葡萄糖：4.98mmol/L（2022-10-14上海市上农医院）。Rbc：4.31×10<sup>12</sup>/L，Hb：133g/L，Wbc：5.86×10<sup>9</sup>/L，中性粒比例：59.6%，淋巴细胞比例：31.5%，Plt：199×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.63mmol/L（2022-10-14上海市上农医院），甘油三酯：1.03mmol/L，高密度脂蛋白：1.38mmol/L，总胆固醇：5.59mmol/L。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：67u/L（2022-10-14上海市上农医院），总胆红素：8.3umol/L，肌酐：61umol/L，尿素氮：6.01mmol/L，尿酸：296umol/L（2022-10-14上海市上农医院），心电图：窦性心律 T波低平（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：脂肪肝（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'509', N'1226', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆瑞0876，女，69岁，智力低下数十年该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活洗漱、进食、洗浴、穿脱衣、如厕等可独立完成。本次入院体检提示高脂血症、窦性心动过缓。患者目前精神状态可，食欲、大小便、睡眠较正常。发热肝炎、结核传染病史，有慢性胃炎史。体检：体温 37℃，脉搏59次/分，呼吸20次/分，血压126/69mmHg。两肺呼吸音清，59次/分，心律齐， 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.45mmol/L（2022-10-14上海市上农医院）。Rbc：4.52×10<sup>12</sup>/L，Hb：134g/L，Wbc：4.65×10<sup>9</sup>/L，中性粒比例：51.2%，淋巴细胞比例：43.2%，Plt：303×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.63mmol/L（2022-10-14上海市上农医院），甘油三酯：0.98mmol/L，高密度脂蛋白：2.05mmol/L，总胆固醇：6.18mmol/L。谷丙转氨酶：19u/L，碱性磷酸酶（ALP）：82u/L（2022-10-14上海市上农医院），总胆红素：8.2umol/L，肌酐：52umol/L，尿素氮：5.21mmol/L，尿酸：230umol/L（2022-10-14上海市上农医院），心电图：窦性心动过缓 左室电压增高（轻）（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'514', N'1229', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆珍0889，女，64岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑。日常生活进食可独立完成，洗漱、穿脱衣、行走、如厕需他人协助完成。本次入院体检提示高脂血症。患者目前精神状态可，食欲、大小便、睡眠较正常。否认肝炎、结核等传染病史。体检：体温 36.6℃，脉搏78次/分，呼吸20次/分，血压94/64mmHg。两肺呼吸音清，78次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：4.92mmol/L（2022-10-14上海市上农医院）。Rbc：5.6×10<sup>12</sup>/L，Hb：121g/L，Wbc：5.74×10<sup>9</sup>/L，中性粒比例：59.8%，淋巴细胞比例：33.8%，Plt：189×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：2.00mmol/L（2022-10-14上海市上农医院），甘油三酯：1.81mmol/L，高密度脂蛋白：1.69mmol/L，总胆固醇：4.88mmol/L。谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：118u/L（2022-10-14上海市上农医院），总胆红素：7.3umol/L，肌酐：43umol/L，尿素氮：3.80mmol/L，尿酸：165umol/L（2022-10-14上海市上农医院），心电图：正常心电图（2022-10-14上海市上农医院）X线胸片：（-）（2022-10-14上海市上农医院）B超：（-）（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'518', N'1244', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆香0938，女，76岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗漱、进食、穿脱衣服、行走、如厕、洗浴等能够独立完成。本次入院体检提示高脂血症。 无高血压病、糖尿病史体检：体温 36.7℃，脉搏77次/分，呼吸20次/分，血压114/73mmHg。两肺呼吸音清。心率77次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.69mmol/L（南京艾迪康医学检验所，2022-10-14）。Rbc：4.47×10<sup>12</sup>/L，Hb：138g/L，Wbc：6.37×10<sup>9</sup>/L，中性粒比例：47.8%，淋巴细胞比例：44.4%，Plt：187×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14）。低密度脂蛋白：3.10mmol/L（南京艾迪康医学检验所，2022-10-14），甘油三酯：2.06mmol/L，高密度脂蛋白：1.19mmol/L，总胆固醇：5.21mmol/L。谷丙转氨酶：19u/L，碱性磷酸酶（ALP）：74u/L（南京艾迪康医学检验所，2022-10-14），总胆红素：12.1umol/L，肌酐：64umol/L，尿素氮：5.78mmol/L，尿酸：240umol/L（南京艾迪康医学检验所，2022-10-14），心电图：正常（上海市上农医院，2022-10-14）X线胸片：正常（上海市上农医院，2022-10-14）B超：正常（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'528', N'1218', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆淑0857，女，69岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示血脂、血压偏高，脂肪肝等。否认肝炎结核病史。体检：体温 36.9℃，脉搏69次/分，呼吸20次/分，血压134/73mmHg。两肺呼吸音粗，心率69次/分，心律齐，腹软，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.95mmol/L（上海市上农医院2022.10.14）。Rbc：4.66×10<sup>12</sup>/L，Hb：144g/L，Wbc：6.82×10<sup>9</sup>/L，中性粒比例：65.1%，淋巴细胞比例：26.2%，Plt：274×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：3.77mmol/L，甘油三酯：2.03mmol/L，高密度脂蛋白：1.39mmol/L，总胆固醇：5.97mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：20u/L，碱性磷酸酶（ALP）：109u/L，总胆红素：13umol/L（上海市上农医院2022.10.14），肌酐：65umol/L，尿素氮：6.52mmol/L，尿酸：262umol/L（上海市上农医院2022.10.14），心电图：窦性心律，偶发室早，T波改变（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：脂肪肝伴多发囊肿（上海市上农医院2022.10.14），梅毒抗体（-）（上海市上农医院2022.10.14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'564', N'1287', N'患者家属', N'基本可靠', N'胡新志', N'2023-07-12 00:00:00.000', NULL, NULL, N'', N'', N'蒋春林，男，85岁，反复头晕头痛四年余。患者四年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服氨氯地平片，血压控制稳定，维持在110-150/70-90之间。于2023年4月9日不慎跌倒，腰部活动受限，松江中心医院诊断：“胸12椎体压缩性骨折、腰4椎体向前I°滑脱”。入院体检提示：慢性支气管炎、肺大泡，肺结节，脂肪肝、肝脏多发囊肿，双肾囊肿、左肾小结石。患者一般情况尚可，轮椅推入病房，反应迟钝，听力减退，查体合作，饮食可，睡眠尚可，大便正常，小便频繁。否认肝炎、结核等传染病史体检：体温 36.8℃，脉搏105次/分，呼吸18次/分，血压132/77mmHg。两肺呼吸音清80次/分，心律齐，无腹壁紧张，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.58mmol/L（2023-07-04松江中心医院）。Rbc：5.11×10<sup>12</sup>/L，Hb：144g/L，Wbc：7.31×10<sup>9</sup>/L，中性粒比例：59.20%，淋巴细胞比例：32.70%，Plt：299×10<sup>9</sup>/L（2023-07-04松江中心医院）。糖化血红蛋白：5.20%（2023-07-04松江中心医院），低密度脂蛋白：3.55mmol/L（2023-07-04松江中心医院），甘油三酯：1.52mmol/L，高密度脂蛋白：0.80mmol/L，总胆固醇：4.71mmol/L。Ca：2.22mmol/L，Cl：102.28mmol/L，K：4.22mmol/L，Na：139mmol/L，P：0.92mmol/L（2023-07-04松江中心医院）。A/G：1.46，白蛋白：35.59g/L，谷丙转氨酶：7.08u/L，间接胆红素：7.83umol/L，碱性磷酸酶（ALP）：104.44u/L（2023-07-04松江中心医院），总胆红素：13.05umol/L，总蛋白：60.05g/L。球蛋白：24.46g/L。直接胆红素：5.22umol/L。肌酐：76.54umol/L，尿素氮：5.47mmol/L，尿酸：253.9umol/L（2023-07-04松江中心医院），白细胞：（-）u/L，管型：（-）u/L，尿胆原：（+），葡萄糖：（-）mmol/L，酸碱度：（-），酮体：（-），上皮细胞：（-）（2023-07-04松江中心医院）u/L，亚硝酸盐：（-），隐血：（-）。心电图：窦性心动过速 心电轴左偏 ST-T改变X线胸片：(缺)。B超：脂肪肝、肝脏多发囊肿 双肾囊肿、左肾小结节 胆囊、阴性、脾脏未见明显占位（2023-07-04松江中心医院）CT：头颅CT:老年脑改变。胸部CT：1.慢性支气管病变，右肺下叶肺大泡2.两肺多发结节3.前纵膈区占位4.主动脉硬化（2023-07-04松江中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'565', N'1288', N'患者本人及家属', N'可信', N'庄秋丽', N'2023-07-25 00:00:00.000', NULL, NULL, NULL, NULL, N'冯引娥，女，90岁，患者右肩关节痛不适半月。患者半月前坐马桶方便时不慎偏倒，致右肩部外伤，感右肩部痛，当时未就诊，来本院要求入院前体检时（于2023.7.19松江中心医院）检查发现右肩关节脱位，当时中心医院医师告知家属及患者本人手法复位时引起肱骨骨折、肋骨骨折、神经损伤可能，急需手术治疗，但最终未做手术治疗，目前予以外固定悬吊固定，被告知脱位无法矫正，可能引起畸形，活动受累。目前患者右肩部痛，右手臂抬举活动受限。患者5个月前不明原因引起两下肢行走移步困难，当时未就诊，目前需搀扶着尚能慢步行走。患者原有两膝关节退变5年余，平时经常酸痛发作。患者入院前体检（中心医院）CT：右肩关节脱位，两膝关节退变，双基底节区腔隙灶，脑白质变性，老年脑改变，慢性支气管炎，右肺中叶结节，主动脉及冠状动脉硬化。B超：左肾囊肿。目前患者一般情况可，无发热，纳食可，两便无异常，夜眠安，基本生活不能自理。体检：体温 36.8℃，脉搏74次/分，呼吸20次/分，血压134/74mmHg。神清，精神可，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，右肩部压痛，红肿不明显，右手臂活动受限，两肺呼吸音清，未及明显干湿啰音，心率74次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢无水肿，两下肢肌力3级，各关节功能活动可。生化:葡萄糖：6.1mmol/L。Rbc：4.25×10<sup>12</sup>/L，Hb：132g/L，Wbc：6.68×10<sup>9</sup>/L，中性粒比例：73.40%，淋巴细胞比例：19.90%，Plt：298×10<sup>9</sup>/L。糖化血红蛋白：5.00%，低密度脂蛋白：2.93mmol/L，甘油三酯：1.51mmol/L，高密度脂蛋白：1.46mmol/L，总胆固醇：4.58mmol/L。Cl：100.54mmol/L，K：5.10mmol/L，Na：138.14mmol/L，A/G：1.58，白蛋白：43.61g/L，谷丙转氨酶：12.19u/L，间接胆红素：7.52umol/L，碱性磷酸酶（ALP）：107.53u/L，总胆红素：12.38umol/L，总蛋白：71.29g/L。球蛋白：27.68g/L。直接胆红素：4.86umol/L。肌酐：61.30umol/L，尿素氮：3.46mmol/L，尿酸：191.67umol/L。心电图：窦性心律，左室肥大，ST-T改变，（ST V5-V6水平压低0.05-0.10mv，T I avL V4-V6低平或负正双向）。B超：肝内钙化灶，左肾囊肿，胆囊、胰腺、脾脏、右肾未见明显占位CT：右肩关节脱位，两膝关节退变，双基底节区腔隙灶，脑白质变性，老年脑改变，慢性支气管炎，右肺中叶结节，主动脉及冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'661', N'1383', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-05-12 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'663', N'1385', N'患者家属', N'基本可信', N'周佳明', N'2025-05-20 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'665', N'1387', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-05-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'674', N'1396', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-09-04 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'395', N'1117', N'患者家属', N'', N'袁纯兰', N'2021-01-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'516', N'1237', N'其他', N'基本可靠', N'张丁', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆寒0910，女，80岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，在日常生活中吃饭、穿衣、洗澡、如厕等方面能够独立完成。入院前体检提示血脂偏高。有梅毒史，有血糖偏高、高脂血症史体检：体温 36.7℃，脉搏105次/分，呼吸20次/分，血压138/89mmHg。两肺呼吸音清。心率105次/分，律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：7.94mmol/L（南京艾迪康医学检验所，2022-10-14）   。Rbc：4.73×10<sup>12</sup>/L，Hb：139g/L，Wbc：6.39×10<sup>9</sup>/L，中性粒比例：58.6%，淋巴细胞比例：35.5%，Plt：248×10<sup>9</sup>/L（南京艾迪康医学检验所，2022-10-14） 。低密度脂蛋白：4.53mmol/L（南京艾迪康医学检验所，2022-10-14） ，甘油三酯：2.63mmol/L，高密度脂蛋白：1.67mmol/L，总胆固醇：7.31mmol/L。谷丙转氨酶：20u/L，碱性磷酸酶（ALP）：129u/L（南京艾迪康医学检验所，2022-10-14） ，总胆红素：8.2umol/L，肌酐：59umol/L，尿素氮：6.11mmol/L，尿酸：285umol/L（南京艾迪康医学检验所，2022-10-14） ，心电图：正常（上海市上农医院，2022-10-14） X线胸片：正常（上海市上农医院，2022-10-14） B超：脂肪肝（上海市上农医院，2022-10-14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'517', N'1252', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'', N'', N'宋民和0979，男，73岁，智力低下几十余年。患者为上海市救助二站受助人员，今转入本院。患者为聋哑人，自幼智力低下，在日常生活吃饭、洗澡、穿衣、如厕等方面能够完全自理，行走缓慢。入院前体检提示中性粒细胞百分比及白细胞偏高。患者原有高血压史，目前长期口服复方卡托普利治疗。既往陈旧性结核、肝炎史粗，无干啰音、无哮鸣音、无湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。80次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 双下肢象皮肿，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.03 mmol/L（南京艾迪康医学检验所）。Rbc：4.9 ×10<sup>12</sup>/L（南京艾迪康检验所），Hb：150g/L，Wbc：18.94×10<sup>9</sup>/L，中性粒比例：87.4%，淋巴细胞比例：9.5%，Plt：282×10<sup>9</sup>/L。C反应蛋白：未检MG/L，糖化血红蛋白：未检%，低密度脂蛋白：2.7mmol/L，甘油三酯：1.09mmol/L（南京艾迪康检验所），高密度脂蛋白：1.41mmol/L，总胆固醇：4.41mmol/L。谷丙转氨酶：13u/L（南京艾迪康检验所），碱性磷酸酶（ALP）：75u/L，总胆红素：25.3umol/L，肌酐：88umol/L，尿素氮：5.92mmol/L（南京艾迪康检验所），尿酸：430umol/L，尿胆原：未检，心电图：正常心电图X线胸片：两肺陈旧性病灶B超：脂肪肝。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'520', N'1210', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民进0415，男，61岁，智力低下几十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活中洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等方面均能自理完成。入院前体检提示尿酸、血脂偏高。因高血压长期服用复方卡托普利片及硝苯地平控释片。患者目前精神状态可，食欲、大小便、睡眠较正常。无肝炎、血吸虫等传染病史。体检：体温 37.2℃，脉搏69次/分，呼吸20次/分，血压140/87mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。80次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为2级,左下肢肌力为2级,右上肢肌力为2级,右下肢肌力为2级。生化:葡萄糖：4.23mmol/L（南京艾迪康医学检验所）。Rbc：5.83×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：176g/L，Wbc：7.55×10<sup>9</sup>/L，中性粒比例：74.7%，淋巴细胞比例：17.4%，Plt：185×10<sup>9</sup>/L。糖化血红蛋白：未检%，低密度脂蛋白：2.68mmol/L，甘油三酯：2.43mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.4mmol/L，总胆固醇：4.63mmol/L。K：未检mmol/L，谷丙转氨酶：21u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：90u/L，总胆红素：16.2umol/L，肌酐：109umol/L，尿素氮：5.92mmol/L（南京艾迪康医学检验所），尿酸：486umol/L，心电图：窦性心律、电轴左偏X线胸片：心影增大B超：胆囊未显示。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'562', N'1285', N'患者家属', N'可信', N'庄秋丽', N'2023-07-06 00:00:00.000', NULL, NULL, NULL, NULL, N'钟祥珍，女，77岁，自知力认知能力差3年。患者2020年3月7日因脑溢血于上海金山中心医院手术治疗，之后出现自知力认知能力差，记忆力差，反映迟钝，3年来外出迷路，时清时糊涂，时连家人都不认识，基本生活不能自理。2023年5月9日于上海市松江区精神卫生中心就诊，诊断：阿尔茨海默症。     
患者脑溢血手术治疗同时发现有高血压病，具体血压高达多少家属诉不详，平时常服用硝苯地平缓释片及阿利沙坦酯片降压，据说血压控制可。
2023年4月24日上海市第六人民医院金山分院体检报告：肝硬化，脂肪肝，肝内多发小囊肿，甲状腺小结节。 
患者目前一般情况可，无发热，纳食可，两便无异常，夜间睡眠可。                   患者原有高血压病3年，脑溢血史3年，幼年5-6时左眼失明。
体检：体温 36.9℃，脉搏72次/分，呼吸20次/分，血压105/69mmHg。神清，精神可，右眼巩膜清，视力可，左眼见白色絮状物覆盖，失明，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，未及明显干湿啰音，心率72次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢无水肿，左上肢肌力为5级，左下肢肌力为5级，右上肢肌力为5级，右下肢肌力为5级。生化:葡萄糖：4.78mmol/L。Rbc：4.40×10<sup>12</sup>/L，Hb：128g/L，Wbc：4.60×10<sup>9</sup>/L，中性粒比例：50.5%，淋巴细胞比例：40.4%，Plt：224×10<sup>9</sup>/L。糖化血红蛋白：6.10%，低密度脂蛋白：2.71mmol/L，甘油三酯：1.99mmol/L，高密度脂蛋白：1.03mmol/L，总胆固醇：4.60mmol/L。Cl：107.1mmol/L，K：3.96mmol/L，Na：144.7mmol/L，A/G：1.6，白蛋白：39.7g/L，谷丙转氨酶：13u/L，间接胆红素：7.4umol/L，碱性磷酸酶（ALP）：147u/L，总胆红素：9.1umol/L，总蛋白：64.6g/L。球蛋白：24.9g/L。直接胆红素：1.7umol/L。肌酐：63umol/L，尿素氮：4.52mmol/L，尿酸：307umol/L，白细胞：0u/L，管型：未见u/L，尿胆原：阴性(-)，葡萄糖：阴性(-)mmol/L，酸碱度：6.5，酮体：阴性(-)，上皮细胞：0-1u/L，亚硝酸盐：阴性(-)，隐血：阴性(-)。心电图：正常心电图。B超：中度脂肪肝CT：老年脑，脑室引流中，左侧眼球多发钙化，左肺上叶、左肺门区多发钙化灶，肝硬化，脂肪肝，肝内多发小囊肿，甲状腺右叶低密度小结节。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'567', N'1291', N'患者家属', N'基本可靠', N'胡新志', N'2023-08-25 00:00:00.000', NULL, NULL, N'', N'', N'蒋福金，女，85岁，记忆力减退，行为紊乱3年。患者3年前开始出现记忆力进行性减退，断断续续不认识家人，答非所问，情绪尚稳定，日夜颠倒，大小便失禁。时有无故独自外出，不能准确说出家庭住址。吃饭需他人辅助，无饥饱，不分生食熟食。总觉得自己东西被偷，时有藏东西行为，无猜疑、被害妄想。在松江精神卫生中心诊断“阿尔茨海默病”。予以多奈哌齐片、富马酸喹硫平片治疗。目前因年事已高、个人生活不能自理于2230825自愿入住我院。患者一般情况尚可，搀扶步入病房，表情淡漠，简单对答，查体合作，饮食可、睡眠欠佳，大小便正常。发热肝炎、结核传染病史体检：体温 36.7℃，脉搏72次/分，呼吸18次/分，血压110/63mmHg。两肺呼吸音清，72次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。双足背轻度水肿生化:葡萄糖：7.65mmol/L）。Rbc：4.01×10<sup>12</sup>/L，Hb：123g/L，Wbc：5.05×10<sup>9</sup>/L，中性粒比例：54.3%，淋巴细胞比例：38.8%，Plt：124×10<sup>9</sup>/L。糖化血红蛋白：6.60%，低密度脂蛋白：2.63mmol/L。甘油三酯：0.80mmol/L，高密度脂蛋白：1.06mmol/L，总胆固醇：3.87mmol/L。Ca：2.18mmol/L，Cl：106mmol/L，K：3.63mmol/L，Na：142mmol/L，P：1.01mmol/LA/G：1.30，白蛋白：38.1g/L，谷丙转氨酶：9.80u/L，间接胆红素：8.39umol/L，碱性磷酸酶（ALP）：59.9u/L，总胆红素：16.27umol/L，总蛋白：67.51g/L。球蛋白：29.41g/L。直接胆红素：7.88umol/L。肌酐：71umol/L，尿素氮：4.29mmol/L，尿酸：337.8umol/L心电图：1.窦性心律2.T波改变II、III、aVF、V3-V6低平。B超：餐后胆囊显示不清。右肾液性占位--囊肿可能 肝脏、胰腺、脾脏左肾未见明显异常。CT：胸部CT：1.右肺上叶实性微小结节2.主动脉冠状动脉硬化。（上海第五康复医院2023-08-18）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'579', N'1302', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2023-12-21 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'592', N'1315', N'患者家属', N'可信', N'王屹', N'2024-03-25 00:00:00.000', NULL, NULL, NULL, NULL, N'金国权，男，78岁，左侧肢体活动不利二年。患者二年前突发左侧肢体活动不利伴口齿不清，即送松江区中心医院诊治，诊断：脑出血，对症治疗后病情基本稳定，予吡拉西坦片等治疗，并在康复医院进行肢体康复训练。患者在治疗期间，出现记忆力、定时、定向能力减退，理解能力欠佳等症状，诊断：混合型阿尔兹海默病，予奥氮平片、喹硫平等治疗。因患者目前生活自理能力基本丧失，今由家属送入本院住养。患者本次入院体检尿酸偏高。
患者目前精神状态可，情绪较稳定，食欲正常，大小便尚正常。患者因高血压、冠心病十余年予酒石酸美托洛尔片、缬沙坦等治疗，因抑郁状态数年予盐酸帕罗西汀片等治疗。睡眠障碍数年间断服用右佐匹克隆，脑脓肿引流术后五年，骨质疏松症三年。体检：体温 36.3℃，脉搏82次/分，呼吸18次/分，血压105/83mmHg。两肺呼吸音粗，心率82次/分，律齐，无腹壁紧张，无压痛，无反跳痛，无肿块，四肢无水肿，左侧肢体偏瘫,左上肢肌力为0级,左下肢肌力为1级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.59mmol/L（2024.3.11中山街道卫生中心）。Rbc：4.59×10<sup>12</sup>/L，Hb：132g/L，Wbc：3.45×10<sup>9</sup>/L，中性粒比例：47.7%，淋巴细胞比例：42.9%，Plt：154
×10<sup>9</sup>/L（2024.3.11中山街道卫生中心）。糖化血红蛋白：5.61%，低密度脂蛋白：2.52
mmol/L（2024.3.11中山街道卫生中心），甘油三酯：1.79mmol/L，高密度脂蛋白：0.96mmol/L，总胆固醇：4.02mmol/L。Ca：2.21
mmol/L（2024.3.11中山街道卫生中心），Cl：104mmol/L，K：3.89mmol/L，Na：149.11mmol/L，A/G：1.76，白蛋白：40.38g/L，谷丙转氨酶：3.58u/L，碱性磷酸酶（ALP）：70.84
u/L（2024.3.11中山街道卫生中心），总胆红素：10.94umol/L，总蛋白：63.32g/L。球蛋白：22.94g/L。直接胆红素：4.09umol/L。肌酐：7.33umol/L，尿素氮：6.61mmol/L，尿酸：438.22
umol/L（2024.3.11中山街道卫生中心），白细胞：+
u/L（2024.3.10中山街道卫生中心），尿胆原：-，葡萄糖：-mmol/L，酸碱度：7，酮体：-，上皮细胞：
u/L，隐血：-。心电图：正常（2024.3.10中山街道卫生中心）X线胸片：(缺)。B超：右肾结石（2024.3.11中山街道卫生中心）CT：1、两肺无活动性病变2、主动脉硬化（2024.3.11中山街道卫生中心）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'593', N'1316', N'患者本人及家属', N'基本可靠', N'胡新志', N'2024-03-27 00:00:00.000', NULL, NULL, N'', N'', N'沈金囡，女，80岁，反复头晕头痛8年。患者八年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服氨氯地平片，血压控制稳定，维持在100-130/70-90之间，服用2年高血压药物后血压控制好自行停药。于2020年5月无诱因出现左侧肢体乏力、走路不稳到第一人民医院就诊，诊断：“腔隙性脑梗”，予以口服药物治疗（具体药物不详），病情稳定，未遗留肢体功能障碍。目前因年事已高，个人生活不能自理，于20240327自愿入住我院，患者一般情况尚可，轮椅推入病房，简单对答，查体合作，饮食可，睡眠尚可，大便难解，小便正常。体检：体温 36.3℃，脉搏78次/分，呼吸18次/分，血压123/73mmHg。两肺呼吸音清，78次/分，心律不齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块，双下肢无浮肿，左下肢肌力3级，右下肢肌力4级。生化:葡萄糖：5.84mmol/L。糖化血红蛋白：4.50%，低密度脂蛋白：3.80mmol/L，甘油三酯：1.58mmol/L，高密度脂蛋白：1.50mmol/L，总胆固醇：5.59mmol/L。Ca：2.20mmol/L，Cl：97.20mmol/L，K：3.69mmol/L，Na：139.39mmol/L，P：1.14mmol/L。A/G：1.54，白蛋白：41.64g/L，谷丙转氨酶：5.34u/L，间接胆红素：9.01umol/L，碱性磷酸酶（ALP）：115.49u/L。肌酐：75.18umol/L，尿素氮：3.45mmol/L，尿酸：235.95umol/L心电图：窦性心律，频发房性早搏，T波改变 II III avF V4-V6低平或负正方向。B超：肝脏弥漫性病变，胆囊、胰腺、脾脏、双肾未见明显占位。头颅CT:左基底节腔隙灶，老年脑改变。胸部CT:1.左肺上叶及右肺中叶结节2.心脏增大，主动脉硬化3.两侧胸腔少量积液。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'599', N'1322', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-04-22 00:00:00.000', NULL, NULL, NULL, NULL, N'黄郁明，男，89岁，反复头晕头痛30余年。患者30余年前无诱因下出现头晕头痛，视物旋转，黑朦，无恶心呕吐，无意识障碍，于市东医院就诊，最高血压160/80mmHg，诊断“高血压病”，予对症治疗（具体不详），病情好转后出院。发病以来，患者无头晕头痛，无发热，无呕吐，无明显消瘦等，饮食睡眠可。 
   体检：体温 37.3℃，脉搏88次/分，呼吸20次/分，血压144/87mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。88次/分，心律不齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。左手食指前端、中指前端、无名指前端部分缺失心电图：心房颤动 ST-T改变 ST V5 V6 水平型压低 0.05-0.075mv ⅠⅡⅢ avF V5 V6 低平（20240415松江区中心医院）X线胸片：无B超：脂肪肝、肝脏囊肿 胆囊息肉 双肾囊肿 胰腺、脾脏未见明显占位（20240415松江区中心医院）CT：头颅CT：1.双侧基底节区腔隙灶，请结合MR检查2.老年脑改变3.左侧上颌窦炎症（20240415松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'601', N'1324', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-04-28 00:00:00.000', NULL, NULL, N'', N'', N'张金方，女，71岁，记忆力减退5年，加重3年。患者约2016年开始出现记忆力下降，易忘事，2021年开始加重，下楼后回家时不记得所在楼层，外出后不认识回家的路，在松江区精神卫生中心就诊，诊断为阿尔茨海默病（老年前期型）予以盐酸多奈哌齐口服，后因心情不好、半夜不睡继续复诊给予加服舍曲林片、盐酸曲唑酮等口服后夜眠差、心情不好症状好转，2023年因记忆力减退明显加重予以加用美金刚口服液治疗。因生活不能完全自理，今入住我院，发病以来，患者无慢性发热，无头痛呕吐，无胸闷气急等，饮食可，睡眠尚可。既往2021年脑梗死病史，右侧第3前肋、左侧第2、3、7前肋骨折后，时间不详。体检：体温 36.5℃，脉搏85次/分，呼吸20次/分，血压141/72mmHg。心率85次/分，心律齐，腹软，无压痛。生化:葡萄糖：5.32mmol/L。低密度脂蛋白：3.36mmol/L，甘油三酯：1.05mmol/L，高密度脂蛋白：2.27mmol/L，总胆固醇：5.83mmol/L（2024.4.21松江区中心医院）。白蛋白：43.61g/L，谷丙转氨酶：17.3u/L，碱性磷酸酶（ALP）：216.22u/L，总胆红素：8.81umol/L，总蛋白：68.12g/L。球蛋白：24.51g/L。肌酐：61.19umol/L，尿素氮：4.28mmol/L，尿酸：203.56umol/L。心电图：正常心电图。B超：左肾囊肿，左肾微小结石，肝脏、胆囊、胰腺、脾脏、右肾未见明显占位CT：双侧放射冠区腔隙灶，老年脑改变，右侧第3前肋、左侧第2、3、7前肋骨折伴骨痂形成，右肺中叶小结节，主动脉及部分冠状动脉硬化（2024.4.21松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'570', N'1293', N'患者家属', N'基本可靠', N'胡新志', N'2023-09-08 00:00:00.000', NULL, NULL, N'', N'', N'陆木锦，男，80岁，反复头晕头痛30余年。患者三十年前无诱因反复出现头晕头痛伴胸闷不适，无气促，无恶心、呕吐，无视物旋转，无黑朦，在松江中心医院就诊，诊断“高血压病、冠状动脉粥样硬化性心脏病”，长期奥美沙坦酯氨氯地平片、卡维地洛片等药物，症状缓解，血压维持在110-150/70-90mmHg之间病情控制稳定。10个月前因胆囊恶性肿瘤在上海市第一人民医院行胆管支架置入术。现因年事已高，步态不稳，个人生活不能自理，于20230908自愿入住我院。患者一般情况尚可，扶入病房，记忆力减退，简单对答，查体合作，饮食可、睡眠尚可，小便正常，有便秘现象。发否认肝炎、结核等传染病史。体检：体温 36.5℃，脉搏64次/分，呼吸18次/分，血压135/65mmHg。两肺呼吸音粗，64次/分，心律齐，腹软，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.39mmol/L。糖化血红蛋白：7.5%，低密度脂蛋白：1.01mmol/L，甘油三酯：1.23mmol/L，高密度脂蛋白：0.55mmol/L，总胆固醇：2.64mmol/L。Ca：1.91mmol/L，Cl：97.3mmol/L，K：3.59mmol/L，Na：133mmol/L，P：0.56mmol/L，A/G：1.24，白蛋白：33.66g/L，谷丙转氨酶：11.17u/L，间接胆红素：7.69umol/L，碱性磷酸酶（ALP）：181u/L，总胆红素：21.79umol/L，总蛋白：60.83g/L。直接胆红素：14.1umol/L。肌酐：44umol/L，尿素氮：3.57mmol/L，尿酸：148umol/L，心电图：正常心电图。B超：胆囊炎，胆总管指甲植入术后，双肾囊肿，肝脏、胰腺，脾脏未见明显占位。CT：胸部CT:主动脉及冠状动脉硬化，右肺中叶微小结节，两肺慢性炎症，左侧基底节区腔隙性梗塞；老年脑。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'572', N'1295', N'患者家属', N'可信', N'庄秋丽', N'2023-09-12 00:00:00.000', NULL, NULL, NULL, NULL, N'张正华，男，83岁，四肢功能障碍4年余。患者2019年时因两下肢酸痛麻木无力于松江第一人民医院就诊，经检查诊断：腰椎椎管狭窄，同时予以手术治疗，因手术失败，目前两下肢仍感麻木酸痛无力，现不能独自站立行走，搀扶尚能缓慢移步。
患者今年起自知力认知能力进行性下降。今年4月曾于松江精神卫生中心就诊，诊断“精神障碍”，而予以奥氮平口服治疗。4月时曾一度出现忧郁状态，出现厌世情绪。
患者帕金森症4年，平时无明显四肢等抖动，曾一度出现晚间惊叫，严重失眠等现象，目前药物控制着。
患者前列腺增生10年，长期服用抗前列腺增生药，效果欠佳，因为小便点滴不清，严重影响睡眠，自今年4月份起予以留置导尿。
患者2013年时曾因剧烈头晕不适于松江第一人民医院就诊，经CT及核磁共振检查提示“脑梗塞”，据说后恢复正常，没留下后遗症。
患者患有癫痫20-30年，四肢反复出现抽搐现象，目前予以药物丙戊酸钠片控制。
患者目前一般情况尚可，无发热，纳食可，大便无异常，留置导尿，导尿管畅，夜间睡眠可。
体检：体温 36.8℃，脉搏92次/分，呼吸20次/分，血压96/56mmHgmmHg。神清，精神可，呼吸平稳，巩膜清，结膜无异常，唇不绀，颈软，颈静脉无怒张，两肺呼吸音粗，未及明显干湿啰音，心率92次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，四肢无水肿，两上肢肌力4级，两下肢肌力3级，各关节功能活动尚可，肌肉萎缩不明显。生化:葡萄糖：5.53mmol/L。Rbc：3.75×10<sup>12</sup>/L，Hb：123.3g/L，Wbc：7.12×10<sup>9</sup>/L，中性粒比例：69.67%，淋巴细胞比例：17.28%，Plt：228.3×10<sup>9</sup>/L。C反应蛋白：3MG/L，Cl：95.17mmol/L，K：4.33mmol/L，Na：130.55mmol/L，谷丙转氨酶：12.85u/L，总胆红素：8.54umol/L，直接胆红素：3.96umol/L。肌酐：69.66umol/L，尿素氮：8.9mmol/L，尿酸：152.75umol/L，白细胞：8-9u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：8，酮体：阴性（-），亚硝酸盐：+，隐血：阴性（-）。心电图：窦性心律，完全性右束支阻滞。B超：脂肪肝，肝小囊肿，胆囊壁毛糙增厚，双肾结石，双肾囊肿，胰腺、脾脏未见明显占位。CT：右肺中叶磨玻璃结节，两肺上叶轻度肺气肿，左侧斜裂增厚，心脏略增大，心包少量积液，主动脉及冠状动脉硬化，甲状腺左叶低密度结节，右肾结石，右肾囊性灶。老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'583', N'1306', N'患者家属', N'可靠', N'袁纯兰', N'2024-01-08 00:00:00.000', NULL, NULL, N'', N'', N'李国民，男，79岁，记忆力进行新减退伴易发脾气三年半。患者三年半前无明显诱因下出现记忆减退，进行新加重，发展至做事情前做后忘，生活全护理，家人名字不记得，外出后不认识回家的路，同时伴易发脾气。2023年10月在松江区中心医院就诊，诊断为：老年痴呆。具体治疗不详。12月在松江区精神卫生中心就诊，诊断为老年痴呆症，予以多奈哌齐片口服。发病以来，患者无发热，无胸闷气急，无呕吐腹泻等，饮食可，睡眠尚可。体检：体温 36.5℃，脉搏98次/分，呼吸20次/分，血压129/70mmHg。两肺呼吸音粗，心率98次/分，心律齐，腹软，无压痛。四肢活动可。Rbc：3.66×10<sup>12</sup>/L，Hb：114g/L，Wbc：8.41×10<sup>9</sup>/L，中性粒比例：74.4%，淋巴细胞比例：18.9%，Plt：197×10<sup>9</sup>/L。糖化血红蛋白：5.7%；低密度脂蛋白：1.99mmol/L，甘油三酯：0.61mmol/L，高密度脂蛋白：1.7mmol/L，总胆固醇：3.72mmol/L；Cl：102.05mmol/L，K：4.13mmol/L，Na：140.94mmol/L。A/G：1.25，白蛋白：39.64g/L，谷丙转氨酶：9.66u/L，间接胆红素：8.72umol/L，碱性磷酸酶（ALP）：76.81u/L，总胆红素：14.97umol/L，总蛋白：71.28g/L，直接胆红素：6.25umol/L。肌酐：66.63umol/L，尿素氮：6.2mmol/L，尿酸：213.83umol/L。心电图：窦性心动过速，T波改变。X线胸片：右肺下叶团块状软组织密度灶，建议增强CT进一步检查。慢性支气管炎，肺气肿，右肺中叶及右肺下叶感染，右肺下叶钙化灶，心脏稍增大，主动脉及冠状动脉硬化，右侧胸腔少量积液，右侧第4-6前肋骨折后改变，肝脏多发囊性灶。B超：肝脏多发囊性灶，肝内钙化灶，胰腺主胰管显示，左肾钙质沉着，脾脏、右肾未见明显占位，餐后胆囊显示不清。CT：老年脑改变（2023.12.29松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'584', N'1307', N'患者本人及家属', N'基本可靠', N'朱晓霞', N'2024-01-12 00:00:00.000', NULL, NULL, N'', N'', N'高仁芳，女，91岁，反复头晕头痛8年。患者8年前无诱因下出现头晕头痛，视物旋转，黑朦，无恶心呕吐，无意识障碍，于松江区中心医院就诊，最高血压160/100mmHg，诊断“高血压病”，予对症治疗（具体不详），病情好转后出院。发病以来，患者无头晕头痛，无发热，无呕吐，无明显消瘦等，饮食睡眠可。 
   患者因高血压病、脑供血不足、焦虑状态目前服用缬沙坦胶囊、盐酸氟桂利嗪胶囊、右佐匹克隆片、草酸艾司西酞普兰片。脑供血不足6年余，慢性胆囊炎、慢性胃炎4-5年，冠心病2年余。体检：体温 36.3℃，脉搏83次/分，呼吸20次/分，血压125/72mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无湿啰音。83次/分，心律齐，未闻及早搏。无腹壁紧张，无压痛，无反跳痛。无腹壁紧张，无压痛，无反跳痛。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。左髋部见一12cm陈旧性疤痕生化:葡萄糖：6.09mmol/L。糖化血红蛋白：5.9%，低密度脂蛋白：4.24mmol/L，甘油三酯：1.79mmol/L，高密度脂蛋白：1.69mmol/L，总胆固醇：6.42mmol/L。Ca：2.25mmol/L，Cl：101.38mmol/L，K：4.13mmol/L，Na：142.2mmol/L，P：1.16mmol/L。A/G：1.52，白蛋白：42.19g/L，谷丙转氨酶：8.17u/L，碱性磷酸酶（ALP）：70.12u/L，总蛋白：70.03g/L。球蛋白：27.84g/L。肌酐：83.52umol/L，尿素氮：7.25mmol/L，尿酸：357.66umol/L，心电图：窦性心律 房性早搏 ST改变 ST V2-V6似弓背型抬高0.05-0.10mv T波改变 II III avF低平X线胸片：(缺)。B超：肝内钙化灶 胆囊、胰腺、脾脏、双肾未见明显占位CT：胸部CT：1.慢性支气管病变2.心影增大，主动脉及冠状动脉硬化。头颅CT：脑白质变性、老年脑改变，必要时建议MR检查。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'600', N'1323', N'患者家属', N'基本可靠', N'胡新志', N'2024-04-25 00:00:00.000', NULL, NULL, N'', N'', N'王桂香，女，90岁，反复头晕头痛20余年。患者二十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服苯磺酸氨氯地平片，血压控制稳定。于2018年突然意识不清、昏迷在家中，大小便失禁，家人发现后即送松江中心医院救治，诊断：“脑出血”。住院治疗好转出院，未遗留肢体功能障碍。现因年事已高，个人生活不能自理，于20240425自愿入住我院。患者一般情况尚可，轮椅推入病室，对答切题，查体合作，饮食可，睡眠尚可，大小便正常。否认肝炎、结核传染病史体检：体温 36.9℃，脉搏64次/分，呼吸18次/分，血压143/78mmHg。两肺呼吸音清，心率64次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。右膝关节肿大。生化:葡萄糖：5.33mmol/L。糖化血红蛋白：5.30%，低密度脂蛋白：3.14mmol/L，甘油三酯：0.58mmol/L，高密度脂蛋白：1.72mmol/L，总胆固醇：4.78mmol/L。Ca：2.20mmol/L，Cl：104.42mmol/L，K：3.80mmol/L，Na：142.11mmol/L，P：1.13mmol/L。白蛋白：41.33g/L，谷丙转氨酶：8.38u/L，间接胆红素：11.53umol/L总胆红素：19.08umol/L，直接胆红素：7.55umol/L。肌酐：61.83umol/L，尿素氮：6.30mmol/L，尿酸：260.48umol/L，白细胞：-u/L，尿胆原：-，葡萄糖：-mmol/L，酸碱度：5.5，酮体：-，亚硝酸盐：-，隐血：+。心电图：房性早搏 T波改变，B超：肝脏囊肿、胆囊炎、胆囊结石、胆囊内胆泥淤积、双肾囊肿、胰腺主胰管显示、脾脏未见明显占位。CT：头颅CT:左侧丘脑区、两基底节-放射冠区腔隙灶，老年脑改变。胸部CT:1.两肺淤血表现，心脏增大，主动脉、冠状动脉硬化。2.胆囊结石，左肾上腺结节，左肾囊性灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'608', N'1331', N'患者家属', N'基本可靠', N'袁纯兰', N'2024-05-22 00:00:00.000', NULL, NULL, N'', N'', N'王美玲，女，91岁，时有头晕头痛两月余。患者2024年3月以来出现头晕头痛伴记忆力减退，在松江区中心医院就诊，测收缩压最高可达165mmhg，诊断为：高血压病、脑梗死。予以盐酸贝尼地平片和吡拉西坦片口服，后血压趋于平稳，头晕头痛好转。因年事已高，记忆力减退伴生活不能完全自理今入住我福利院。发病以来患者无发热，无腹痛呕吐，无胸闷气急等，饮食睡眠可。患者既往骨质疏松症目前服用碳酸钙片治疗。体检：体温 36.7℃，脉搏69次/分，呼吸20次/分，血压135/68mmHg。两肺呼吸音清粗，心率69次/分，心律齐，腹软，无压痛，生化:葡萄糖：5.21mmol/L。糖化血红蛋白：5.5%，低密度脂蛋白：2.62mmol/L，甘油三酯：1.87mmol/L，高密度脂蛋白：1.09mmol/L，总胆固醇：4.14mmol/L（2024.4.12上海市第五康复医院）。Cl：104.7mmol/L，K：3.72mmol/L，Na：142mmol/L，A/G：1.58，白蛋白：43.2g/L，谷丙转氨酶：9.1u/L，间接胆红素：13.01umol/L，碱性磷酸酶（ALP）：66.3u/L，总胆红素：19.29umol/L，总蛋白：70.52g/L，直接胆红素：6.28umol/L。肌酐：64.3umol/L，尿素氮：4.48mmol/L，尿酸：383.7umol/L（2024.4.12上海市第五康复医院。心电图：窦性心动过缓，完全性右束支传导阻滞，电轴左偏。B超：脂肪肝，胆囊多发结石，左肾小囊肿，胰腺、脾脏、右肾未见明显异常。CT：两肺散在轻度炎症，心脏增大，胆囊结石，两侧放射冠区腔隙灶，老年脑改变（2024.4.12上海市第五康复医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'610', N'1333', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-05-28 00:00:00.000', NULL, NULL, N'', N'', N'曹逸仙，女，77岁，记忆力减退1年，加重1月。患者2023年来出现记忆力减退，易忘事，言语重复，刚说的话做过的事情就记不清，吃饭不知饥饱，2023年2月在上海市第一人民医院就诊，CT提示双侧脑室旁、额顶叶多发腔隙灶、缺血灶，脑萎缩。予以石杉碱甲片口服。2023年5月左膝关节置换术、左股骨骨折内固定术后因生活不能自理，担心，多虑，害怕摔跤，情绪易激动，爱哭，夜间睡不着，2024年4月在松江区精神卫生中心就诊，诊断为混合性痴呆、焦虑状态、睡眠障碍，予以盐酸舍曲林片、艾司唑仑口服后症状好转，现夜眠可未服用艾司唑仑片。今生活不能自理入住我院，发病以来，患者无慢性发热，无头痛呕吐，无胸闷气急等，饮食可，睡眠尚可。2023年内左侧膝关节置换术后、2023年左侧股骨骨折内固定术。体检：体温 36.6℃，脉搏66次/分，呼吸20次/分，血压117/65mmHg。两肺呼吸音粗，心率67次/分，心律齐，腹软，无压痛，。生化:葡萄糖：5.09mmol/L。糖化血红蛋白：4.7%，低密度脂蛋白：2.87mmol/L，甘油三酯：2.66mmol/L，高密度脂蛋白：1.03mmol/L，总胆固醇：4.59mmol/L（2024.5.16上海市第五康复医院）。Cl：106.2mmol/L，K：3.86mmol/L，Na：141.6mmol/L，A/G：1.22，白蛋白：41.7g/L，谷丙转氨酶：22.7u/L，碱性磷酸酶（ALP）：47.2u/L，总胆红素：10.07umol/L，总蛋白：78.89g/L。肌酐：63.9umol/L，尿素氮：4.67mmol/L，尿酸：357.1umol/L。心电图：窦性心动过缓。B超：脂肪肝，胆囊、胰腺、脾脏、双肾未见明显异常。CT：脑白质病，老年脑改变，额骨骨瘤，主动脉及冠状动脉硬化（2024.5.16上海市第五康复医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'611', N'1334', N'患者家属', N'可靠', N'袁纯兰', N'2024-05-29 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'614', N'1337', N'患者家属', N'可信', N'王屹', N'2024-06-13 00:00:00.000', NULL, NULL, NULL, NULL, N'樊林根，男，76岁，右侧肢体活动不利六年。患者六年前无明显诱因下突发右侧肢体活动不利伴头晕、口齿不清，经松江区中心医院诊治，诊断为：脑梗死，予对症治疗后病情稍稳定，患者目前予中药治疗。患者发病后生活自理能力逐渐减退，近一年来记忆力下降，有时情绪波动较大。本次入院体检提示：双肺多发结节、肺气肿、完全性右束支传导阻滞、脂肪肝等。患者目前精神状态可，食欲、睡眠、大小便较正常，今由家属送本院住养。患者原有高血压史二十余年，一年前停止治疗；冠心病、高脂血症数年；慢性胃炎、便秘数年；腰椎手术后十余年。体检：体温 36.6℃，脉搏88次/分，呼吸18次/分，血压127/76mmHg。两肺呼吸音清，心率88次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，右侧肢体偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为1级,右下肢肌力为0级。生化:葡萄糖：9.55mmol/L（随机）
(2024.5.19松江区中心医院)。低密度脂蛋白：3.58
(2024.6.5松江区中心医院)mmol/L，甘油三酯：3.09mmol/L，高密度脂蛋白：1.03mmol/L，总胆固醇：5.62mmol/L。(2024.5.19松江区中心医院)mmol/L。A/G：1.47，白蛋白：41g/L，谷丙转氨酶：65u/L，间接胆红素：1umol/L，碱性磷酸酶（ALP）：88
(2024.5.19松江区中心医院)u/L，总胆红素：2.7umol/L，总蛋白：68.9g/L。直接胆红素：1.7umol/L。肌酐：103umol/L，尿素氮：5.8mmol/L，尿酸：385
(2024.5.19松江区中心医院)umol/L，心电图：完全性右束支传导阻滞
(2024.5.19松江区中心医院)B超：脂肪肝
(2024.6.5松江区中心医院)CT：双肺多发结节；肺气肿；主动脉、冠状动脉硬化；双侧基底节、放射区散在腔隙灶，老年脑
(2024.5.19松江区中心医院)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'617', N'1340', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-06-20 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'603', N'1326', N'患者家属', N'可靠', N'袁纯兰', N'2024-05-07 00:00:00.000', NULL, NULL, N'', N'', N'陈志强，男，77岁，记忆力减退五年，加重一年。患者约约5年开始出现记忆力下降，易忘事，2023年开始加重，不认识自己的儿女，外出后不认识回家的路，2023年6月在复旦大学附属中山医院就诊，诊断为阿尔茨海默病，先后予以甘露特纳胶囊、吡拉西坦、盐酸多奈哌齐片治疗，2023年10月因睡眠障碍予以加服右佐匹克隆片或者阿普唑仑片，服药后睡眠改善。因生活不能完全自理，今入住我院，发病以来，患者无慢性发热，无头痛呕吐，无胸闷气急等，饮食可，睡眠尚可。患者因静脉曲张目前服用柑橘黄酮片和马栗种子提取物片。体检：体温 36.8℃，脉搏65次/分，呼吸20次/分，血压128/64mmHg。两肺呼吸音清，心率65次/分，心律齐，腹软，无压痛，无反跳痛，双下肢静脉曲张。生化:葡萄糖：5.4mmol/L，Rbc：4.52×10<sup>12</sup>/L，Hb：139g/L，Wbc：5.08×10<sup>9</sup>/L，中性粒比例：48.6%，淋巴细胞比例：44.5%，Plt：133×10<sup>9</sup>/L，糖化血红蛋白：5.2%，低密度脂蛋白：3.57mmol/L，甘油三酯：0.95mmol/L，高密度脂蛋白：1.06mmol/L，总胆固醇：5.02mmol/L，Cl：102.29mmol/L，K：4.06mmol/L，Na：139.05mmol/L，A/G：1.6，白蛋白：41.6g/L，谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：70u/L，总胆红素：21umol/L，总蛋白：67.8g/L，直接胆红素：3.4umol/L。肌酐：82umol/L，尿素氮：4.95mmol/L，尿酸：256umol/L（2024.4.19上海市第八人民医院），白细胞：1u/L，管型：0u/L，尿胆原：（-），葡萄糖：+++mmol/L，亚硝酸盐：（-），隐血：弱阳性。心电图：窦性心律，电轴左偏，左前分支传导阻滞，顺钟向转位。B超：胆囊结石，肝脏、胰腺、脾脏、双肾未见明显异常。CT：两侧基底节区缺血灶，老年脑，左肺上叶磨玻璃结节影，两肺下叶少许慢性炎症及条索，右肺下叶钙化灶，主动脉及冠脉钙化，少量心包积液（2024.4.19上海市第八人民医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'605', N'1327', N'患者家属', N'基本可信', N'庄秋丽', N'2024-05-10 00:00:00.000', NULL, NULL, NULL, NULL, N'林正文，男，85岁，患者思维紊乱、胡言乱语6年余。患者6年前开始出现认知能力下降，思维紊乱，胡言乱语，当时未就诊未检查。1年半前两下肢行走欠稳欠利，但能扶手可独自缓慢行走。半年前起两下肢无力，不能独自站立行走，目前坐轮椅活动。
患者2024年3月20日因恶心呕吐、纳差于小昆山卫生服务中心就诊，B超检查：胆囊结石，之后又曾发作过一次，静脉点滴后好转。
患者本次入院前检查：头颅CT:左侧小脑半球及左枕部软化灶，两侧基底节腔隙灶。老年脑，脑白质变性。腹部CT:脂肪肝，肝脏钙化灶，胆囊多发小结石，胆囊胆汁潴留，胸12椎体压缩性骨折改变。
患者目前一般情况尚可，无发热，纳食可，两便无异常，基本生活不能自理。
体检：体温 36.4℃，脉搏88次/分，呼吸20次/分，血压129/78mmHg。神志清，精神可，检查不配合，体温36.4°C，BP129/78mmHg，氧饱和度97%，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率88次/分，律齐，腹软，全腹无压痛，尾骶部见直径约2cmx2cm左右皮损，少量渗液，无红肿，四肢无水肿，两下肢肌力约2级，各关节功能活动可。生化:葡萄糖：6.33mmol/L。Rbc：4.26×10<sup>12</sup>/L，Hb：127g/L，Wbc：5.84×10<sup>9</sup>/L，中性粒比例：62.80%，淋巴细胞比例：25.20%，Plt：321×10<sup>9</sup>/L。C反应蛋白：3.67MG/L，糖化血红蛋白：5.90%，低密度脂蛋白：3.40mmol/L，甘油三酯：1.98mmol/L，高密度脂蛋白：0.59mmol/L，总胆固醇：4.65mmol/L。Ca：0.94mmol/L，Cl：97.49mmol/L，K：3.34mmol/L，Na：138.20mmol/L，P：0.94mmol/L。A/G：1.02，白蛋白：34.69g/L，谷丙转氨酶：36.88u/L，间接胆红素：4.21umol/L，碱性磷酸酶（ALP）：105.08u/L，总胆红素：7.42umol/L，总蛋白：68.59g/L。球蛋白：33.90g/L。直接胆红素：3.21umol/L。肌酐：55.45umol/L，尿素氮：4.08mmol/L，尿酸：229.35umol/L。心电图：窦性心律，心电轴左偏，完全性右束支传导阻滞，顺钟向转位，T波改变，I AVL V4-V6低平、倒置、左前分支阻滞。CT：胸部：慢性支气管炎改变，左肺上叶、两肺下叶少许纤维灶，心脏增大，主动脉及冠状动脉硬化，胆囊多发结石。头颅：头颅CT:左侧小脑半球及左枕部软化灶，两侧基底节腔隙灶。老年脑，脑白质变性。腹部CT:脂肪肝，肝脏钙化灶，胆囊多发小结石，胆囊胆汁潴留，右肾小结节，双肾囊性灶，右侧间位结肠，胸12椎体压缩性骨折改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'606', N'1329', N'患者家属', N'可信', N'王屹', N'2024-05-16 00:00:00.000', NULL, NULL, NULL, NULL, N'沈礼国，男，79岁，双下肢活动不利五年余。患者五年前无明显诱因下突发双下肢活动不利，由家属送至松江区中心医院就诊，诊断：脑梗死，予对症治疗后病情基本稳定。二年前患者生活自理能力逐步减退，长期卧床。患者原有帕金森病七年予多巴丝肼片治疗；前列腺增生六年予盐酸坦索罗辛缓释胶囊治疗；高血压半年予美阿沙坦钾片治疗；便秘多年予开塞露治疗。本次入院体检提示：心脏增大冠脉硬化；双侧部分肋骨陈旧性骨折；食管裂孔疝；胆囊结石；肾囊肿；老年脑改变。患者目前精神状态尚可，食欲、大小便较正常，睡眠尚可。今由家属送至本院住养。颈椎间盘突出数年，九年前因腹部疝手术治疗。体检：体温 36.6℃，脉搏92次/分，呼吸18次/分，血压146/87mmHg。两肺呼吸音粗，心率92次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，下腹部见手术疤痕，阴囊肿大，四肢无水肿，左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为3级。B超：胆囊炎、胆囊结石、双肾囊肿（2024.3.26松江区中心医院）CT：右肺少量炎症；心脏增大，主动脉及部分冠脉硬化；双侧部分肋骨陈旧性骨折；食管裂孔疝（2024.3.27松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'615', N'1339', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-06-20 00:00:00.000', NULL, NULL, N'', N'', N'赵国兴，男，83岁，记忆力减退五年，加重一年余。患者五年前因头晕头痛伴记忆力减退，刚说过的话就忘记，不知道饥饱，不记得吃饭，外出后不记得回家的路，但认识自己家人。在松江中心医院就诊，诊断为：脑梗死、阿尔茨海默病、认知障碍等，予以脑心通胶囊、阿司匹林肠溶片、多奈哌齐等口服，头痛头晕缓解。2023年3月开始记忆力减退加重，继续在松江区中心医院复诊，予以加服美金刚口溶膜片治疗。因年事已高今入住我福利院。发病以来患者无慢性发热，无胸闷气急，无呕吐腹痛等，饮食可，睡眠偶有不佳。
患者既往前列腺增生病史，目前服用非那雄胺片治疗。体检：体温 36.6℃，脉搏74次/分，呼吸20次/分，血压141/80mmHg。两肺呼吸音清，心率74次/分，心律齐，腹软，无压痛，双下肢浮肿。生化:葡萄糖：5.03mmol/L。甘油三酯：1.44mmol/L，总胆固醇：3.91mmol/L，Cl：98.95mmol/L，K：4.27mmol/L，Na：141.25mmol/L，A/G：1.83，白蛋白：42.98g/L，谷丙转氨酶：9.97u/L，间接胆红素：6.28umol/L，碱性磷酸酶（ALP）：69.36u/L，总胆红素：10.56umol/L，总蛋白：66.5g/L。直接胆红素：4.38umol/L。肌酐：87.09umol/L，尿素氮：7.4mmol/L，尿酸：266.75umol/L。心电图：窦性心律，t波改变。B超：肝内实性结节（小血管瘤可能），胆囊、脾脏、双肾未见明显占位，胰腺显示不清，CT：老年脑改变，心脏增大，主动脉及冠状动脉硬化（2024.6.14松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'618', N'1341', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-06-26 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'620', N'1343', N'患者家属', N'可信', N'王屹', N'2024-07-02 00:00:00.000', NULL, NULL, NULL, NULL, N'吴元根，男，69岁，左侧肢体活动不利半年。患者半年前无明显诱因下突发左侧肢体活动不利，伴言语不清，经松江区中心医院诊治，诊断为：脑梗死，予右侧大脑中动脉颅内取栓术、阿司匹林肠溶片抗血小板凝聚、阿托伐他汀钙片稳定斑块等对症治疗，症状基本稳定。患者发病后理解力、记忆力、计算力逐步减退，左上肢活动障碍，生活自理能力明显下降，今由家属送入本院住养。患者目前精神状态可，口齿稍含糊，纳可，睡眠可，大小便较正常。患者左手腕骨折术后数年，高血压史二年余，目前未服药。体检：体温 36.6℃，脉搏71次/分，呼吸18次/分，血压125/84mmHg。两肺呼吸音粗，心率71次/分，心律齐，未闻及早搏,无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 四肢无水肿，无肌肉萎缩，左上肢肌力为0级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.02
(松江区中心医院2024.6.26)mmol/L。Rbc：4.39×10<sup>12</sup>/L，Hb：130g/L，Wbc：6.32×10<sup>9</sup>/L，中性粒比例：59.9%，淋巴细胞比例：29.4%，Plt：207
(松江区中心医院2024.6.26)×10<sup>9</sup>/L，低密度脂蛋白：1.74
(松江区中心医院2024.6.26)mmol/L，甘油三酯：0.87mmol/L，高密度脂蛋白：1.13mmol/L，总胆固醇：3.07mmol/L。谷丙转氨酶：15.49u/L，间接胆红素：6.98umol/L，碱性磷酸酶（ALP）：61.38
(松江区中心医院2024.6.26)u/L，总胆红素：11.44umol/L，总蛋白：71.26g/L。球蛋白：29.04g/L。直接胆红素：4.46umol/L。肌酐：80.92umol/L，尿素氮：5.41mmol/L，尿酸：335.38
(松江区中心医院2024.6.26)umol/L，心电图：窦性心律、异常Q波（Ⅱ、Ⅲ、aVF）
(松江区中心医院2024.6.26)B超：肝左叶肝内胆管扩张
(松江区中心医院2024.6.26)CT：右肺上叶及左肺斜裂小结节；心脏轻度增大，主动脉、冠状动脉壁部分钙化；右侧大脑中动脉术后改变，右侧大脑半球多发软化灶，老年脑改变
(松江区中心医院2024.6.26)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'624', N'1346', N'患者家属', N'可信', N'王屹', N'2024-07-12 00:00:00.000', NULL, NULL, NULL, NULL, N'房龙友，男，80岁，时有头晕不适二十余年。患者二十余年前经常出现头晕头痛不适，经松江区中心医院诊断为：高血压，予苯磺酸氨氯地平片治疗后血压基本稳定。患者平时服药欠规则，未定期监测血压。患者一年前因口齿不清，肢体乏力，外院诊断为：脑血管病，目前无特殊治疗。患者六年前因双手震颤，外院诊断为：帕金森病，予多巴丝肼片治疗。本次入院体检提示：低血钾、主动脉及冠状动脉硬化、肺结节、胆囊息肉、老年脑等。患者目前精神状态尚可，四肢震颤明显，双下肢浮肿，行动缓慢，生活自理能力明显减退，基本卧床，食欲尚可，经常便秘，予便通胶囊等治疗，睡眠差。今由家属送至本院住养。
腰椎间盘突出术后二年，前列腺增生数年。体检：体温 36.5℃，脉搏73次/分，呼吸18次/分，血压109/70mmHg。两肺呼吸音粗，心率73次/分，心律齐，无腹壁紧张，无压痛，四肢震颤，肌张力增强，双下肢浮肿，肛周皮肤潮红，部分皮损。生化:葡萄糖：5.26
mmol/L（松江区中心医院2024.7.8）。Rbc：4.35×10<sup>12</sup>/L，Hb：135g/L，Wbc：6.08×10<sup>9</sup>/L，中性粒比例：72.7%，淋巴细胞比例：20.1%，Plt：223
×10<sup>9</sup>/L（松江区中心医院2024.7.8）。低密度脂蛋白：3.87
mmol/L（松江区中心医院2024.7.8），甘油三酯：0.95mmol/L，高密度脂蛋白：1.15mmol/L，总胆固醇：5.24mmol/L。Ca：2.39mmol/L，Cl：98.19mmol/L，K：3.35mmol/L，Na：139.37mmol/L，P：0.97
mmol/L（松江区中心医院2024.7.8）。A/G：1.66，白蛋白：44.76g/L，谷丙转氨酶：4.66u/L，间接胆红素：8.39umol/L，碱性磷酸酶（ALP）：55.04
u/L（松江区中心医院2024.7.8），总胆红素：14.32umol/L，总蛋白：71.8g/L。球蛋白：27.04g/L。直接胆红素：5.93umol/L。肌酐：108umol/L，尿素氮：9.03mmol/L，尿酸：357
umol/L（松江区中心医院2024.7.8），B超：胆囊息肉
（松江区中心医院2024.7.8）CT：左肺下叶实性结节，主动脉及冠状动脉硬化；老年脑
（松江区中心医院2024.7.8）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'633', N'1355', N'患者家属', N'基本可靠', N'胡新志', N'2024-09-19 00:00:00.000', NULL, NULL, N'', N'', N'张苏芳，女，72岁，反复头晕头痛20余年。患者二十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服苯磺酸氨氯地平片，血压控制尚稳定。2012年因多饮多食，诊断：“糖尿病2型”，长期服用西格列汀二甲双胍片，血糖控制在6.8-8.0之间。2017年、2024年先后2次突然右侧肢体乏力、活动障碍，无意识障碍，松江中心医院诊断：“脑梗死”予以住院治疗（具体治疗不详），病情好转，遗留右侧肢体活动不利。近半年长期在上海市第五康复医院康复治疗。体检：体温 37.2℃，脉搏89次/分，呼吸18次/分，血压122/84mmHg。两肺呼吸音清，89次/分，心律齐，无腹壁紧张，四肢无水肿，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：7.03mmol/L。糖化血红蛋白：6.30%低密度脂蛋白：1.61mmol/L，甘油三酯：1.75mmol/L，高密度脂蛋白：0.76mmol/L，总胆固醇：2.69mmol/L。Cl：104.8mmol/L，K：4.20mmol/L，Na：143.4mmol/L，A/G：2.14，白蛋白：42.10g/L，谷丙转氨酶：14.80u/L，间接胆红素：13.72umol/L，总胆红素：25.64umol/L，总蛋白：61.80g/L。球蛋白：19.70g/L。直接胆红素：11.92umol/L。肌酐：54.1umol/L，尿素氮：5.59mmol/L，尿酸：347.0umol/L，心电图：心电图：窦性心律 T波改变B超：B超：1.肝外胆管扩张，下段肠气干扰未探及2.左肾多发性囊肿。头颅CT:1.左侧额叶软化灶2.双侧基底节、放射冠区腔梗。胸部CT:1.左肺下叶钙化小结节2.主动脉及冠脉钙化：纵膈多发淋巴结影。心包少量积液。3.附件：甲状腺右侧叶低密度结节影。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'635', N'1357', N'患者家属', N'可信', N'王屹', N'2024-10-14 00:00:00.000', NULL, NULL, NULL, NULL, N'何立新，男，74岁，咳嗽咳痰不畅加剧一月余。患者原有慢性支气管炎十余年，今年9月起咳嗽咳痰不畅加剧，住上海市闵行区中心医院治疗，诊断：肺炎、低蛋白血症等，予吸氧、亚胺培南抗感染、氨溴索止咳化痰、全能力鼻饲等对症支持治疗，患者症状较前略缓解，今予出院入住本院。患者目前一般情况差，卧床不起，明显消瘦，问之不答，喉部痰鸣明显，伴阵发性气促，建议家属转院诊治，家属表示放弃，并签病重病危通知书。腔隙性脑梗死3年，冠状动脉粥样硬化性心脏病5年，双侧髋关节退行性改变数年。体检：体温 38.1℃，脉搏103次/分，呼吸20次/分，血压107/73mmHg。两肺呼吸音粗，闻及湿啰音。心率103次/分，心律齐。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 四肢无水肿。生化:葡萄糖：5.1
(上海市闵行区中心医院2024.9.26)mmol/L。Rbc：3.92×10<sup>12</sup>/L，Hb：103g/L，Wbc：5.69×10<sup>9</sup>/L，中性粒比例：69.2%，淋巴细胞比例：18.6%，Plt：467
(上海市闵行区中心医院2024.10.4)×10<sup>9</sup>/L。低密度脂蛋白：1.98
(上海市闵行区中心医院2024.10.11)mmol/L，甘油三酯：1.68mmol/L，高密度脂蛋白：0.91mmol/L，总胆固醇：3.45mmol/L。Cl：104
(上海市闵行区中心医院2024.10.4)mmol/L，K：4.5mmol/L，Na：138mmol/L，A/G：1.8，白蛋白：41g/L，谷丙转氨酶：17u/L，总胆红素：7.0umol/L，总蛋白：64g/L。球蛋白：23g/L。直接胆红素：3.6
(上海市闵行区中心医院2024.10.4)umol/L。肌酐：51umol/L，尿素氮：6.4mmol/L，尿酸：167
(上海市闵行区中心医院2024.10.4)umol/L，心电图：窦性心动过速
(上海市闵行区中心医院2024.9.23)X线胸片：(缺)。B超：肝脏小囊肿、左肾小结石
(上海市闵行区中心医院2024.9.23)CT：两肺散在炎症
(上海市闵行区中心医院2024.9.23)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'642', N'1364', N'患者本人及家属', N'基本可靠', N'朱晓霞', N'2025-02-06 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'649', N'1371', N'患者本人及家属', N'可靠', N'朱晓霞', N'2025-03-25 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'650', N'1372', N'患者本人及家属', N'可靠', N'朱晓霞', N'2025-03-26 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'496', N'1240', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆勤，女，66岁，智力低下数十年。 该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。体检：体温 36.3℃，脉搏89次/分，呼吸20次/分，血压116/71mmHg。两肺呼吸音粗，无干啰音、湿啰音，心率89次/分，心律齐，未闻及早搏，腹软，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常。生化:葡萄糖：4.89mmol/L（2022.10.10上海市上农医院）。Rbc：3.9×10<sup>12</sup>/L，Hb：128g/L，Wbc：6.22×10<sup>9</sup>/L，中性粒比例：52.5%，淋巴细胞比例：34.4%，Plt：208×10<sup>9</sup>/L（2022.10.10上海市上农医院）。低密度脂蛋白：2.92mmol/L，甘油三酯：0.96mmol/L，高密度脂蛋白：1.54mmol/L，总胆固醇：5.03mmol/L（2022.10.10上海市上农医院）。谷丙转氨酶：10u/L，碱性磷酸酶（ALP）：85u/L，总胆红素：12.9umol/L（2022.10.10上海市上农医院），肌酐：53umol/L，尿素氮：5.3mmol/L，尿酸：186umol/L（2022.10.10上海市上农医院），心电图：正常心电图（2022.10.10上海市上农医院）X线胸片：（-）（2022.10.10上海市上农医院）B超：（-）（2022.10.10上海市上农医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'510', N'1227', N'其他', N'基本可靠', N'胡新志', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'刘毛英0882，女，77岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，日常生活吃饭、洗澡、穿衣、行走、如厕可独立完成。本次入院体检提示高胆固醇血症、高脂血症、窦性心动过缓。患者目前精神状态可，食欲、大小便、睡眠较正常。发热肝炎、结核史，有梅毒史。体检：体温 37℃，脉搏55次/分，呼吸20次/分，血压136/87mmHg。两肺呼吸音清，55次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.08mmol/L（2022-10-14上海市上农医院）。Rbc：4.90×10<sup>12</sup>/L，Hb：146g/L，Wbc：7.88×10<sup>9</sup>/L，中性粒比例：73.7%，淋巴细胞比例：20.3%，Plt：272×10<sup>9</sup>/L（2022-10-14上海市上农医院）。低密度脂蛋白：3.16mmol/L（2022-10-14上海市上农医院），甘油三酯：5.46mmol/L，高密度脂蛋白：1.43mmol/L，总胆固醇：1.83mmol/L。谷丙转氨酶：14u/L，碱性磷酸酶（ALP）：83u/L（2022-10-14上海市上农医院），总胆红素：12.5umol/L，肌酐：73umol/L，尿素氮：7.59mmol/L，尿酸：312umol/L（2022-10-14上海市上农医院），心电图：窦性心律过缓 右室传导延迟（2022-10-14上海市上农医院）X线胸片：(-)（2022-10-14上海市上农医院）B超：(-)（2022-10-14上海市上农医院）CT：(缺)。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'513', N'1242', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'陈桂秀，女，65岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以独立完成。本次入院体检提示肺气肿。否认肝炎结核等病史体检：体温 36.5℃，脉搏87次/分，呼吸20次/分，血压133/85mmHg。两肺呼吸音清，心率87次/分，律齐，腹软，无压痛，四肢无水肿，右腿肌肉萎缩，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：5.2mmol/L（上海市上农医院2022.10.14）。Rbc：4.06×10<sup>12</sup>/L，Hb：130g/L，Wbc：5.01×10<sup>9</sup>/L，中性粒比例：62.6%，淋巴细胞比例：31.7%，Plt：252×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：2.8mmol/L，甘油三酯：0.92mmol/L，高密度脂蛋白：1.41mmol/L，总胆固醇：4.86mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：19u/L，碱性磷酸酶（ALP）：112u/L，总胆红素：10.9umol/L（上海市上农医院2022.10.14），肌酐：58umol/L，尿素氮：5.29mmol/L，尿酸：202umol/L（上海市上农医院2022.10.14），心电图：窦性心律，T波改变（上海市上农医院2022.10.14）X线胸片：肺气肿（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）CT：梅毒抗体（-）丙肝抗体（-）（上海市上农医院2022.10.14）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'544', N'1267', N'患者家属', N'基本可靠', N'袁纯兰', N'2023-03-30 00:00:00.000', NULL, NULL, N'', N'', N'李琴芳，女，83岁，头晕头痛反复发作十二余年。患者十二年前无明显诱因下出现头晕头痛，在松江中心医院就诊，诊断为高血压病，给予苯磺酸氨氯地平片服用控制血压，后定期监测血压，在140/80mmhg左右。本次入院体检提示脂肪肝。目前因患者年事渐高，家中无人照顾，于2023年03月30号入住本院。患者目前精神、饮食、睡眠均可，大小便正常。
患者既往糖尿病史，目前给予门冬胰岛素30注射液控制血糖；过敏性鼻炎长期口服盐酸西替利嗪片。体检：体温 36.6℃，脉搏75次/分，呼吸20次/分，血压125/65mmHg。两肺呼吸音清，心率75次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：mmol/L。Rbc：4.83×10<sup>12</sup>/L，Hb：149g/L，Wbc：6.42×10<sup>9</sup>/L，中性粒比例：58.2%，淋巴细胞比例：29.8%，Plt：138×10<sup>9</sup>/L，糖化血红蛋白：8.6%，低密度脂蛋白：3.22mmol/L，甘油三酯：1.84mmol/L，高密度脂蛋白：1.04mmol/L，总胆固醇：4.82mmol/L，Cl：99.66mmol/L（2023.3.28松江区中心医院），K：4.41mmol/L，Na：135.84mmol/L，白蛋白：40.06g/L，谷丙转氨酶：43.04u/L，总胆红素：9.2umol/L，肌酐：55.82umol/L，尿素氮：4.53mmol/L，尿酸：176.59umol/L。心电图：窦性心律，电轴右偏，完全性右束支传导阻滞。B超：脂肪肝，胰腺显示不清，胆囊、脾脏、双肾未见明显占位。CT：脑白质变性，老年脑改变，轻度脑积水，两肺结节，左肺下叶纤维灶，心脏稍增大，主动脉及冠状动脉硬化，脂肪肝（2023.3.28松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'545', N'1268', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-04-12 00:00:00.000', NULL, NULL, N'', N'', N'马根芳，女，81岁，患者因反酸烧心伴恶心呕吐于2023年2月13日入住复旦大学附属华山医院，在2月17日全麻下行腹腔镜下食管裂孔疝修补术，于2月22日出院。患者目前无发酸、烧心、无恶心呕吐等不适。本次入院体检提示左侧放射冠区腔隙灶。因年事渐高，家中无人照看今入住我院。发病以来，患者无发热，无腹痛腹泻，无呕吐，饮食可，睡眠一般。患者腰椎间盘突出症伴腰痛十余年，目前服用双氯芬酸钠缓释胶囊 和强力天麻杜仲胶囊治疗。左侧第8、9、10前肋陈旧性骨折，右侧半月板置换术后体检：体温 36.8℃，脉搏64次/分，呼吸20次/分，血压146/75mmHg。两肺呼吸音粗，，心率80次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：5.81mmol/L。Rbc：4.08×10<sup>12</sup>/L，Hb：110g/L，Wbc：3.37×10<sup>9</sup>/L，中性粒比例：51.7%，淋巴细胞比例：33.8%，Plt：195×10<sup>9</sup>/L。低密度脂蛋白：2.09mmol/L，甘油三酯：1.24mmol/L，高密度脂蛋白：1.09mmol/L，总胆固醇：3.32mmol/L。Ca：2.19mmol/L，Cl：105.54mmol/L，K：4.11mmol/L，Na：142.08mmol/L，P：1.21mmol/L。白蛋白：41.65g/L，谷丙转氨酶：10.38u/L，总胆红素：7.36umol/L，肌酐：67.33umol/L，尿素氮：5.55mmol/L，尿酸：284.57umol/L心电图：窦性心动过速，t波改变X线。B超：肝脏回声稍粗，肝脏囊肿，胆囊附壁小结石，双肾囊肿，左肾小结石，胰腺、脾脏未见明显占位。CT：左侧放射冠区腔隙灶，老年脑改变，左肺上叶下舌段及右肺中叶少许纤维灶，食管裂孔疝，左侧第8、9、10前肋陈旧性骨折（2023.4.4松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'553', N'1277', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-05-24 00:00:00.000', NULL, NULL, N'', N'', N'李小妹，女，88岁，双下肢活动不利两年八月余。患者因2020年9月不慎摔倒后在松江区中心医院住院，诊断为右股骨骨折，予以手术治疗。2023年5月5日因行走不慎再次摔倒，诉髋部疼痛，给予口服双氯芬酸钠缓释片治疗。2023年5月11日因发热咳嗽在松江区中心医院住院治疗，诊断为间质性肺炎，脑梗死，低钾血症。在院期间CT检查提示右股骨骨折后内固定中，右侧耻骨上下肢及左侧耻骨下支骨折后。因个人生活不能完全自理今入住我福利院。本次入院体检提示血糖偏高、双侧基底节、放射冠区腔隙灶，给予阿司匹林肠溶片口服。发病以来，患者无胸闷气急，无恶心呕吐，无腹痛腹泻等不适，饮食可，小便如常，大便难解长给予开塞露，夜眠差长期口服艾司唑仑片。体检：体温 36.4℃，脉搏83次/分，呼吸20次/分，血压146/84mmHg。两肺呼吸音粗，心率83次/分，心律齐，腹软，无压痛，四肢无水肿，无肌肉萎缩，肌张力无异常，左下肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：15.12mmol/L，低密度脂蛋白：2.93mmol/L，甘油三酯：0.96mmol/L，高密度脂蛋白：1.63mmol/L，总胆固醇：4.83mmol/L，Cl：107.7mmol/L，K：3.21mmol/L，Na：145mmol/L，肌酐：59.38umol/L，尿素氮：7.39mmol/L，尿酸：265.47umol/L。心电图：窦性心律，左室肥大，频发室性早搏。B超：肝脏囊肿，胆总管扩张，双肾囊肿。CT：右股骨骨折内固定中，右侧耻骨上下肢及左侧耻骨下支骨折后，慢性支气管炎、两肺散在少许炎症，主动脉及冠状动脉硬化，双侧基底节-放射冠区多发腔隙灶，老年脑（2023年5月19日松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'526', N'1202', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'阮之秀0110，女，78岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下伴双下肢残疾，日常生活中洗漱、进食、洗浴、穿脱衣服、行走、如厕等可以需要协助完成。本次入院体检提示血压偏高，乙型病毒性肝炎，胆囊息肉等。否认结核病史。体检：体温 37.1℃，脉搏100次/分，呼吸20次/分，血压107/72mmHg。两肺呼吸音粗，心率100次/分，心律齐，腹软，无压痛，四肢无水肿，双双下肢残疾。生化:葡萄糖：6.87mmol/L（上海市上农医院2022.10.14）。Rbc：4.28×10<sup>12</sup>/L，Hb：119g/L，Wbc：5.43×10<sup>9</sup>/L，中性粒比例：70.5%，淋巴细胞比例：20.6%，Plt：207×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：3.29mmol/L，甘油三酯：1.14mmol/L，高密度脂蛋白：1.14mmol/L，总胆固醇：5.15mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：18u/L，碱性磷酸酶（ALP）：46u/L，总胆红素：8.5umol/L（上海市上农医院2022.10.14），肌酐：59umol/L，尿素氮：8.53mmol/L，尿酸：173umol/L（上海市上农医院2022.10.14），心电图：窦性心律，STT改变，电轴右偏（上海市上农医院2022.10.14）X线胸片：右侧水平裂增厚（上海市上农医院2022.10.14）B超：胆囊息肉（上海市上农医院2022.10.14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'527', N'1204', N'其他', N'基本可靠', N'袁纯兰', N'2022-12-15 00:00:00.000', NULL, NULL, N'上海市救助二站', N'', N'宋圆娥0138，女，73岁，智力低下数十年。该患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，聋哑，日常生活中洗漱、进食、穿脱衣服、行走等可以独立完成。洗浴、如厕需要协助。本次入院体检提示血脂、血压偏高。否认结核病史。体检：体温 36.5℃，脉搏83次/分，呼吸20次/分，血压134/87mmHg。两肺呼吸音粗，心率83次/分，心律齐，腹软，无压痛，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：4.27mmol/L（上海市上农医院2022.10.14）。Rbc：4.55×10<sup>12</sup>/L，Hb：134g/L，Wbc：6.7×10<sup>9</sup>/L，中性粒比例：66.1%，淋巴细胞比例：29.2%，Plt：169×10<sup>9</sup>/L（上海市上农医院2022.10.14）。低密度脂蛋白：3.56mmol/L，甘油三酯：1.78mmol/L，高密度脂蛋白：1.28mmol/L，总胆固醇：5.36mmol/L（上海市上农医院2022.10.14）。谷丙转氨酶：17u/L，碱性磷酸酶（ALP）：61u/L，总胆红素：19.5umol/L（上海市上农医院2022.10.14），肌酐：64umol/L，尿素氮：7.84mmol/L，尿酸：281umol/L（上海市上农医院2022.10.14），心电图：窦性心律，T波改变（上海市上农医院2022.10.14）X线胸片：（-）（上海市上农医院2022.10.14）B超：（-）（上海市上农医院2022.10.14）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'535', N'1232', N'', N'基本可信', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, NULL, NULL, N'宋圆茹0892，女，64岁，智力低下数十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下。日常生活中可独立完成洗脸刷牙、进食、行走、穿脱衣、洗浴、如厕等。患者因高血压在服硝苯地平片、复方卡托普利片及丹参片，血压控制较稳定。入院前体检提示总胆固醇偏高、低密度脂蛋白偏高；B超示脂肪肝；心电图检查：窦性心律、T波倒置；B超提示：脂肪肝。患者目前精神状态可，食欲、大小便、睡眠较正常。体检：体温 36.6℃，脉搏101次/分，呼吸20次/分，血压102/77mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。101次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.59mmol/L（南京艾迪康医学检验所）。Rbc：4.72×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：141g/L，Wbc：7.33×10<sup>9</sup>/L，中性粒比例：65.6%，淋巴细胞比例：28.5%，Plt：232×10<sup>9</sup>/L。低密度脂蛋白：4.32mmol/L，甘油三酯：1.44mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.72mmol/L，总胆固醇：6.26mmol/L。谷丙转氨酶：30u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：81u/L，总胆红素：10umol/L，肌酐：61umol/L，尿素氮：9.39mmol/L（南京艾迪康医学检验所），尿酸：309umol/L，心电图：窦性心律、T波倒置、心肌缺血X线胸片：（-）B超：脂肪肝。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'537', N'1206', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋圆红0226，女，68岁，智力低下几十年。患者为上海市救助二站受助人员，今入住本院。患者自幼智力低下，无法交流，另因左眼失明自行行走缓慢不稳。日常生活中洗脸、刷牙、进食、行走、穿脱衣、洗浴、如厕等不能完全自理，需要别人协助完成。入院前体检提示：无特殊异常。患者目前精神状态可，食欲、大小便均无异常。体检：体温 36℃，脉搏86次/分，呼吸20次/分，血压103/72mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。86次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.14mmol/L（南京艾迪康医学检验所）。Rbc：3.85×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：116g/L，Wbc：5.41×10<sup>9</sup>/L，中性粒比例：64.6%，淋巴细胞比例：29.3%，Plt：162×10<sup>9</sup>/L。低密度脂蛋白：2.22mmol/L，甘油三酯：0.70mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.72mmol/L，总胆固醇：4.53mmol/L。谷丙转氨酶：15u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：94u/L，总胆红素：7.5umol/L，肌酐：55umol/L，尿素氮：7.43mmol/L（南京艾迪康医学检验所），尿酸：141umol/L，心电图：正常X线胸片：正常B超：正常。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'560', N'1283', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-07-04 00:00:00.000', NULL, NULL, N'', N'', N'沈菊华，女，88岁，时有胸闷5年余入院。患者5年余来无明显诱因下出现胸闷不适，在松江区中心医院就诊，诊断为“冠状动脉粥样硬化性心脏病，室性期前收缩”，给予阿司匹林肠溶片、丹参片、参松养心胶囊、麝香保心丸等口服治疗后胸闷好转，但仍时有发作。2023年6月12日因胸痛发作在松江区中心医院住院治疗，给予阿司匹林抗血小板，普伐他汀稳定斑块，氯沙坦钾降压，参松养心胶囊治疗早搏，丹参活血，烟酰胺营养心肌等治疗后好转出院。患者既往高血压病、便秘、胃炎等口服氯沙坦钾片、猴头菌片、胆宁片等口服。因家属无人照料今入住我福利院，本次入院体检提示左侧基底节区腔隙灶、右侧4-6前肋骨骨折，慢性支气管炎、高尿酸血症等。夜眠差服用艾司唑仑可改善。体检：体温 36.6℃，脉搏92次/分，呼吸20次/分，血压108/73mmHg。两肺呼吸音粗，心率92次/分，心律不齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：6.58mmol/L。低密度脂蛋白：2.1mmol/L，甘油三酯：0.93mmol/L，高密度脂蛋白：1.58mmol/L，总胆固醇：3.87mmol/L。Cl：105.8mmol/L，K：4.38mmol/L，Na：140mmol/L，A/G：1.43，白蛋白：41.5g/L，谷丙转氨酶：11.5u/L，碱性磷酸酶（ALP）：86u/L，总胆红素：7.8umol/L，总蛋白：70.6g/L。肌酐：100umol/L，尿素氮：6.47mmol/L，尿酸：460umol/L。尿常规 白细胞：8u/L，管型：0u/L，葡萄糖：（-）mmol/L，隐血：+。心电图：正常心电图。CT：右侧第4-6前肋骨骨折后改变，胸9、12椎体楔形变，慢性支气管炎，左侧基底节区腔隙灶，老年脑改变（202.6.12松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'529', N'1254', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民德0991，男，73岁，智力低下几十年。患者为上海市救助二站受助人员，今入住本院。患者为盲人疑似精神障碍，日常吃饭、洗澡、穿衣、如厕等方面均无法自理，需要帮助完成，另因患有前列腺增生、尿潴留，长期卧床并留置导尿管，2022-12-14患者由上海市上农医院出院。目前一般情况稳定，无发热，无头晕头痛等不适，纳可，大小便正常。无肝炎、结核、血吸虫等传染病史。体检：体温 37.1℃，脉搏90次/分，呼吸20次/分，血压108/65mmHg。两肺呼吸音粗，无干湿啰音、无哮鸣音、无胸膜摩擦音、无异常呼吸音、语音传导无异常。90次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.14mmol/L（上海市上农医院）。Rbc：4.45×10<sup>12</sup>/L（上海市上农医院），Hb：129g/L，Wbc：9.9×10<sup>9</sup>/L，中性粒比例：67.7%，淋巴细胞比例：29.9%，Plt：236×10<sup>9</sup>/L。C反应蛋白：未检MG/L，糖化血红蛋白：未检%，甘油三酯：1.03mmol/L（上海市上农医院），总胆固醇：4.38mmol/L。Ca：1.22mmol/L，Cl：89.5mmol/L，K：4.8mmol/L（上海市上农医院），Na：133.8mmol/L，白蛋白：33.9g/L，谷丙转氨酶：19u/L（上海市上农医院），碱性磷酸酶（ALP）：129u/L，肌酐：64umol/L，尿素氮：4.8mmol/L（上海市上农医院），尿酸：235umol/L，心电图：窦性心律，伪差X线胸片：两肺紊乱增粗、增多；左侧多发肋骨陈旧性骨折B超：胆囊附壁小结晶；前列腺增生；肝胰脾未见明显异常。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'530', N'1211', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民伟0418，男，65岁，智力低下数十年。患者为上海市救助二站受助人员，今入住本院。患者疑似智力障碍，能听懂语言，但无法交流，日常吃饭、洗澡、穿衣、如厕等方面能够完全自理。入院前体检梅毒阳性、血脂偏高、脂肪肝。目前一般情况稳定，无发热，无头晕、头痛等不适，纳可，大小便正常。梅毒、右小腿下肢静脉曲张伴溃疡。甲状腺功能异常体检：体温 36.2℃，脉搏93次/分，呼吸20次/分，血压124/90mmHg。两肺呼吸音清，无干湿啰音、无哮鸣音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。93次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：7.48mmol/L（南京艾迪康医学检验所）。Rbc：5.46×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：161g/L，Wbc：8.89×10<sup>9</sup>/L，中性粒比例：62.8%，淋巴细胞比例：30.6%，Plt：248×10<sup>9</sup>/L。低密度脂蛋白：4.43mmol/L，甘油三酯：1.94mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.3mmol/L，总胆固醇：6.26mmol/L。谷丙转氨酶：30南京艾迪康医学检验所）u/L，碱性磷酸酶（ALP）：104u/L，总胆红素：13.6umol/L，肌酐：54umol/L，尿素氮：4.71mmol/L（南京艾迪康医学检验所），尿酸：213umol/L，心电图：窦性心律 不完全性右束支传导阻滞X线胸片：（-）B超：脂肪肝。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'561', N'1284', N'患者家属', N'基本可靠', N'胡新志', N'2023-07-05 00:00:00.000', NULL, NULL, N'', N'', N'朱维彬，男，91岁， 反复头晕头痛伴胸闷四年余。 患者四年前无诱因反复出现头晕头痛伴胸闷不适，无气促，无恶心、呕吐，无视物旋转，无黑朦，在松江中心医院就诊，诊断“高血压病、冠状动脉粥样硬化性心脏病”，长期口服氨氯地平片、心脑康胶囊等药物，症状缓解，血压维持在110-160/70-90mmHg之间病情控制稳定。两年前骑三轮车不慎翻车致左股骨颈骨折，行走困难，多坐轮椅。现因年事已高，行动不便，个人生活不能自理，于20230705自愿入住我院。患者一般情况尚可，轮椅推入病房，听力减退，查体合作，饮食可、睡眠尚可，大便正常。否认肝炎、结核病史体检：体温 36.8℃，脉搏74次/分，呼吸18次/分，血压140/72mmHgmmHg。两肺呼吸音清，心率74次/分，心律齐，无腹壁紧张，无压痛，双下肢浮肿明显，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。腰椎骨折术后，左侧股骨颈骨折后，左尺桡骨闭合性骨折后。生化:葡萄糖：5.29mmol/L（2023-6-27松江中心医院）。Rbc：3.33×10<sup>12</sup>/L，Hb：102g/L，Wbc：4.88×10<sup>9</sup>/L，中性粒比例：66.2%，淋巴细胞比例：27.90%，Plt：200×10<sup>9</sup>/L，C反应蛋白：2.47MG/L，糖化血红蛋白：5.50%（2023-6-27松江中心医院），低密度脂蛋白：2.45mmol/L，甘油三酯：1.09mmol/L，高密度脂蛋白：1.24mmol/L，总胆固醇：4.05mmol/L。Ca：2.21mmol/L，Cl：101.61mmol/L，K：2.74mmol/L，Na：143.40mmol/L，P：0.89mmol/L。肌酐：131.87umol/L，尿素氮：5.30mmol/L，尿酸：487.81umol/L，心电图：T波改变 I avL II III avF V3-V6 低平、双相或浅倒置（2023-6-27松江中心医院）X线胸片：(缺)。B超：双肾囊肿，肝脏、胆囊、脾脏未见明显占位，胰脏显示不清（2023-6-27松江中心医院）CT：头颅CT:老年脑改变。胸部CT:1.右肺上叶尖段实性小结节2.双肺下叶轻度炎症可能3.心脏增大 主动脉硬化（2023-6-27松江中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'566', N'1289', N'患者家属', N'基本可靠', N'胡新志', N'2023-08-21 00:00:00.000', NULL, NULL, N'', N'', N'钱进贤，男，89岁，右上肢无力7年余。患者七年前无诱因突发右侧肢体活动不利，无恶心呕吐，无意识障碍，无视物昏花，在松江中心医院就诊，诊断“脑梗死”，具体治疗不详，病情好转后出院，遗留右侧肢体肌力减弱。糖尿病病史3年长期口服二甲双胍片、格列喹酮片，血糖控制可。
目前因年事已高，个人生活不能自理，于20230821自愿入住我院，轮椅推入病房，简单对答，听力减退，查体合作，饮食可，睡眠尚可，大小便正常。否认肝炎、结核等传染病史，否认高血压病史。体检：体温 36.5℃，脉搏108次/分，呼吸18次/分，血压117/63mmHg。两肺呼吸音清，无干湿啰音、108次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为4级,左下肢肌力为3级,右上肢肌力为4级,右下肢肌力为3级。生化:葡萄糖：6.3mmol/L。Rbc：4.17×10<sup>12</sup>/L，Hb：129g/L，Wbc：3.09×10<sup>9</sup>/L，中性粒比例：67.30%，淋巴细胞比例：23.10%，Plt：110×10<sup>9</sup>/L。C反应蛋白：28.65MG/L、低密度脂蛋白：2.91mmol/L甘油三酯：0.74mmol/L，高密度脂蛋白：0.64mmol/L，总胆固醇：3.77mmol/L。Ca：2.27mmol/L（松江中心医院2023-7-30），Cl：99.50mmol/L，K：4.14mmol/L，Na：134mmol/L，A/G：1.08，白蛋白：34.1g/L，谷丙转氨酶：9.8u/L，总胆红素：9.3umol/L（松江中心医院2023-7-31），总蛋白：65.7g/L。肌酐：106.0umol/L，尿素氮：5.54mmol/L，尿酸：5.54umol/L尿胆原：+，葡萄糖：++mmol/L，亚硝酸盐：阴性（松江中心医院2023-8-2），隐血：阴性。心电图：正常心电图CT：头颅CT：左丘脑区腔隙灶，老年脑改变。胸部CT:1.双肺多发结节、斑片影。2.两肺散在纤维钙化灶3.主动脉及冠状动脉硬化。上腹部CT:1.肝左叶囊性灶2.腹主动脉硬化。下腹部CT：膀胱憩室，前列腺增大伴钙化灶。（松江中心医院2023-7-30）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'531', N'1255', N'', N'基本可靠', N'周婷', N'2022-12-15 00:00:00.000', NULL, NULL, N'救助二站', N'', N'宋民水1019，男，72岁，智力低下几十年。患者为上海市救助二站受助人员，今转入本院。患者自幼智力低下，因患有脑梗后遗症右侧肢体功能障碍需长期坐轮椅，在日常生活吃饭、洗澡、穿衣、如厕等方面需帮助完成。患者原有高血压史长期口服厄贝沙坦氢氯噻嗪片控制血压。目前一般情况稳定，无发热，无头晕头痛等不适，纳可，大小便粗，无干湿啰音、无哮鸣音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。77次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为0级,右下肢肌力为0级。生化:葡萄糖：4.73mmol/L（南京艾迪康医学检验所）。Rbc：4.32×10<sup>12</sup>/L（南京艾迪康医学检验所），Hb：132g/L，Wbc：6.79×10<sup>9</sup>/L，中性粒比例：68.7%，淋巴细胞比例：18.3%，Plt：224×10<sup>9</sup>/L。低密度脂蛋白：2.82mmol/L，甘油三酯：0.94mmol/L（南京艾迪康医学检验所），高密度脂蛋白：1.54mmol/L，总胆固醇：4.71mmol/L。谷丙转氨酶：21u/L（南京艾迪康医学检验所），碱性磷酸酶（ALP）：93u/L，总胆红素：11umol/L，肌酐：75umol/L，尿素氮：9.07mmol/L（南京艾迪康医学检验所），尿酸：318umol/L，心电图：窦性心动过缓X线胸片：（-）B超：胆囊未显示。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'552', N'1275', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-05-15 00:00:00.000', NULL, NULL, N'', N'', N'杨忠弟，男，68岁，左侧肢体活动不利17个月。患者2021年12月份因头晕伴意识下降，左侧肢体活动不利在上海市第一人民医院南院住院，诊断为蛛网膜下腔出血、高血压病，ANCA相关性肾炎、肺栓塞、肺部阴影、脑梗死、高脂血症。患者后续多次在上海市阳光康复中心住院治疗，最近一次于2023年4月7日出院。因日常生活不能自理今入住我福利院。本次入院体检提示肺气肿、糖尿病。患者目前一般情况尚可，无发热，无咳嗽气急，无胸闷胸痛，纳食可，两便无异常。体检：体温 36.7℃，脉搏93次/分，呼吸20次/分，血压148/88mmHg。两肺呼吸音粗，心率93次/分，心律齐，腹部软，无压痛，左上肢肌力为3级,左下肢肌力为2级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：mmol/L。Rbc：4.33×10<sup>12</sup>/L，Hb：130g/L，Wbc：8.29×10<sup>9</sup>/L，中性粒比例：65.5%，淋巴细胞比例：26.3%，Plt：233×10<sup>9</sup>/L（2023.5.10上海市第一人民医院）。糖化血红蛋白：7.4%，低密度脂蛋白：4.47mmol/L，甘油三酯：2.13mmol/L，高密度脂蛋白：1.43mmol/L，总胆固醇：6.56mmol/L。Cl：99.7mmol/L，K：4.44mmol/L，Na：140.7mmol/L，A/G：1.36，白蛋白：38.8g/L，谷丙转氨酶：33.4u/L，间接胆红素：7.5umol/L，碱性磷酸酶（ALP）：63.4u/L，总胆红素：10umol/L，总蛋白：67.4g/L。球蛋白：28.6g/L。直接胆红素：2.5umol/L。肌酐：63.1umol/L，尿素氮：6.21mmol/L，尿酸：351.1umol/L（2023.5.10上海市第一人民医院），心电图：窦性心律，左前分支传导阻滞（2023.5.10上海市第一人民医院）X线胸片：(缺)。B超：(缺)。CT：桥脑、右侧基底节区及双侧室旁多发腔隙灶、梗塞灶。脑萎缩，脑白质病，颅内动脉硬化。右肺多发实性结节及肿块，双肺上叶间隔旁型肺气肿，主动脉及冠状动脉硬化，心脏增大（2023.5.10上海市第一人民医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'554', N'1276', N'患者本人及家属', N'可信', N'庄秋丽', N'2023-05-26 00:00:00.000', NULL, NULL, NULL, NULL, N'陆菊芳，女，88岁，患者反复胸闷心悸气促20年。患者原有冠状动脉粥样硬化性心脏病20年，尤其近2年频繁反复出现胸闷心悸气促现象，因此曾多次住院治疗。 患者原有糖尿病20年，注射胰岛素及口服降糖药治疗，据说目前血糖控制好，平时无多饮多食多尿等症状。患者原有肾功能不全20年，本次入院前检查肌酐尿素氮均高。患者本次入院前检查：中心医院CT：双侧放射冠区腔隙灶，老年脑改变。心脏增大，主动脉、冠状动脉壁部分钙化，右肺中叶钙化灶，左肺下叶少许炎症，左侧第7前肋骨折伴骨痂形成。B超：双肾弥漫性病变、双肾囊肿。患者目前一般情况可，无发热，纳食可，稍咳嗽咳痰，气不喘，两便无异常，长期卧床(因去年病后常卧床，引起两下肢无力，不能独自站立行走，需搀扶行走），基本生活不能自理，夜间睡眠差常服用利眠药改善睡眠。
体检：体温 36.5℃，脉搏80次/分，呼吸20次/分，血压BP117/60mmHgmmHg。神清，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音粗，无明显干湿啰音，80次/分，心律不齐，闻及3－5次/分早搏，腹软，全腹无压痛，无反跳痛，肝脾肋下未及肿大，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级，左下肢肌力为3级，右上肢肌力为5级，右下肢肌力为3级。生化:葡萄糖：2.61mmol/L。Rbc：3.27×10<sup>12</sup>/L，Hb：95g/L，Wbc：9.28×10<sup>9</sup>/L，中性粒比例：46%，淋巴细胞比例：43%，Plt：277×10<sup>9</sup>/L。糖化血红蛋白：5.60%，低密度脂蛋白：2.44mmol/L，甘油三酯：0.77mmol/L，高密度脂蛋白：1.50mmol/L，总胆固醇：4.13mmol/L。Ca：2.25mmol/L，Cl：106.56mmol/L，K：4.47mmol/L，Na：144.15mmol/L，P：1.16mmol/L。A/G：1.30，白蛋白：40.23g/L，谷丙转氨酶：7.48u/L，间接胆红素：3.17umol/L，碱性磷酸酶（ALP）：72.58u/L，总胆红素：5.66umol/L，总蛋白：71.10g/L。球蛋白：30.87g/L。直接胆红素：2.49umol/L。肌酐：199.27umol/L，尿素氮：12.87mmol/L，尿酸：470.48umol/L，白细胞：6.00u/L，管型：1.00u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：6.5，酮体：阴性（-），上皮细胞：27u/L，亚硝酸盐：阴性（-），隐血：阴性（-）。心电图：室性早搏。B超：双肾弥漫性病变、双肾囊肿，肝脏、胆囊、胰腺、脾脏未见明显占位。CT：双侧放射冠区腔隙灶，老年脑改变。心脏增大，主动脉、冠状动脉壁部分钙化，右肺中叶钙化灶，左肺下叶少许炎症，左侧第7前肋骨折伴骨痂形成。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'557', N'1280', N'患者本人及家属', N'基本可信', N'庄秋丽', N'2023-06-06 00:00:00.000', NULL, NULL, NULL, NULL, N'谢子森，男，86岁，患者自知力认知能力差3年余。患者近3年渐进性出现胡言乱语，经常前言不搭后语，自知力认知能力差，记忆力逐渐减退，前讲后忘，入院前（本月2日）于松江区精神卫生中心就诊，诊断为：阿尔茨海默病性痴呆。患者原有高血压病20余年，据说平时血压不高，最高血压大概在160/90mmHg左右，最近常服用苯磺酸左氨氯地平片及氯沙坦钾氢氯噻嗪，血压控制可。患者原有前列腺增生症20余年，曾B超检查诊断之，平时小便急，点滴不清，目前常服用盐酸坦索罗辛缓释胶囊。患者10年前突然出现昏迷不醒，于医院就诊经头颅CT检查：脑梗死，经治疗后无后遗症，生活能自理。患者1年前出现渐进性两下肢移步困难，行走跌跌冲冲，目前不能独自行走，只能搀扶缓慢移步，目前病因不明。患者目前一般情况可，无发热，纳食可，稍咳嗽咳痰，痰呈白粘痰，尚能咳出，气不喘，小便急点滴不清，大便难解常用通便药通便。体检：体温 36.8℃，脉搏88次/分，呼吸20次/分，血压130/71mmHg。神清，两肺呼吸音清，心率88次/分，心律齐，腹软，全腹无压痛， 肝脾肋下未及肿大，四肢无水肿，两上肢肌力为5级，两下肢肌力为3级。生化:葡萄糖：6.39mmol/L。Rbc：4.16×10<sup>12</sup>/L，Hb：121g/L，Wbc：5.79×10<sup>9</sup>/L，中性粒比例：68.40%，淋巴细胞比例：19.90%，Plt：232×10<sup>9</sup>/L。糖化血红蛋白：6.10%，低密度脂蛋白：3.16mmol/L，甘油三酯：1.27mmol/L，高密度脂蛋白：0.68mmol/L，总胆固醇：4.34mmol/L。Cl：97.91mmol/L，K：3.54mmol/L，Na：137.13mmol/L，A/G：1.03，白蛋白：36.16g/L，谷丙转氨酶：20u/L，间接胆红素：10.05umol/L，碱性磷酸酶（ALP）：59.21u/L，总胆红素：17.52umol/L，总蛋白：71.21g/L。球蛋白：35.05g/L。直接胆红素：7.47umol/L。肌酐：91.55umol/L，尿素氮：9.05mmol/L，尿酸：383.63umol/L，白细胞：16.00u/L，管型：0.00u/L，尿胆原：+，葡萄糖：阴性mmol/L（-），酸碱度：5.5，酮体：阴性（-），上皮细胞：1.00u/L，亚硝酸盐：阴性（-），隐血：阴性（-）。心电图：正常心电图。B超：肝脏囊肿，左肾囊肿。CT：双侧基底节、放射冠区散在腔隙灶、老年脑、脑白质变性。右肺上叶、两肺下叶炎症，两肺上叶多发小结节，主动脉、冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'542', N'1265', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-03-23 00:00:00.000', NULL, NULL, N'', N'', N'孙益辉，男，80岁，头晕头痛反复发作二十余年入院。患者二十余年前无明显诱因下出现头晕头痛，在松江区中心医院，诊断为“高血压病”。给予氨氯地平片口服，平素监测血压可，两年前调整为厄贝沙坦氢氯噻嗪片口服，近期测血压120/76mmhg。半年来出现记忆力减退，加上患者年事渐高，家中无人照顾入住本院。本次入院体检提示高尿酸血症、脂肪肝、肝囊肿。发病以来患者无胸闷心悸，无发热，无呕吐腹泻等，患者慢性支气管炎长期吸氧治疗，饮食、睡眠均可，大小便正常。体检：体温 36.1℃，脉搏97次/分，呼吸20次/分，血压142/64mmHg。两肺呼吸音粗，心率97次/分，心律齐，腹软，无压痛。生化:葡萄糖：7.24mmol/L。糖化血红蛋白：6.3%，低密度脂蛋白：2.53mmol/L，甘油三酯：0.57mmol/L，高密度脂蛋白：1.82mmol/L，总胆固醇：4.5mmol/L。白蛋白：41.64g/L，谷丙转氨酶：34.02u/L，间接胆红素：5.31umol/L，总胆红素：9.87umol/L，直接胆红素：4.56umol/L。肌酐：102.81umol/L，尿素氮：7.44mmol/L，尿酸：486.25umol/L。心电图：窦性心律，电轴左偏，T波改变。B超：脂肪肝，肝囊肿，胆囊、脾脏、双肾未见明显占位，胰腺显示不清。CT：两侧放射冠区多发腔隙灶，老年脑改变，肺气肿，右肺中叶，左肺上叶下段纤维灶，左肺多发结节，冠状动脉及主动脉硬化，脂肪肝，肝脏多发囊性灶（2023.3.20松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'602', N'1325', N'患者家属', N'基本可靠', N'胡新志', N'2024-05-06 00:00:00.000', NULL, NULL, N'', N'', N'董取英，女，89岁，反复头晕头痛30余年。患者三十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服硝苯地平控释片，血压控制尚稳定。6年前突然出现头晕乏力不适，无意识障碍，在松江中心医院诊断“脑梗死”予以住院治疗（具体治疗不详），病情好转，未遗留肢体功能障碍。现因年事已高、个人生活不能自理，于20240506自愿入住我院。患者一般情况尚可，轮椅推入病室，反应迟钝、简单对答，查体合作，饮食可，睡眠尚可，大小便正常，双足凹陷性水肿明显。体检：体温 36.6℃，脉搏67次/分，呼吸18次/分，血压136/73mmHg。两肺呼吸音清，67次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，双下肢浮肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.34mmol/L。糖化血红蛋白：6.10%低密度脂蛋白：2.23mmol/L，甘油三酯：0.82mmol/L，高密度脂蛋白：1.71mmol/L，总胆固醇：3.86mmol/L。Ca：2.29mmol/L，Cl：103.56mmol/L，K：4.47mmol/L，Na：140.14mmol/L，P：1.37mmol/L。白蛋白：43.70g/L，谷丙转氨酶：8.65u/L，肌酐：87umol/L，尿素氮：6.37mmol/L，尿酸：348.58umol/L，心电图：窦性心律 完全性右束支传导阻滞 T波改变。B超：肝脏囊肿 胆囊炎、胆囊结石、胰腺主胰管显示 双肾测值偏小 脾脏未见明显占位。CT：头颅CT:双侧基底节区-放射冠区腔隙灶，老年脑改变。胸部CT:1.左肺上叶磨玻璃小结节2.右肺中叶少许纤维灶3.心脏增大、主动脉、冠状动脉硬化4.右侧极少量胸腔积液5.双侧多发性肋骨骨折后改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'555', N'1278', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-05-30 00:00:00.000', NULL, NULL, N'', N'', N'庄勤秀，男，89岁，时有头晕头痛20余年。记忆力减退3年，加重半月入院。患者20余年前无明显诱因下出现头晕头痛，在松江区中心医院就诊，诊断为高压病。给予富马酸比索洛尔口服，血压控制尚可。3年前监测血压最高提示180/95mmhg，给予加服奥美沙坦酯氨氯地平片口服后血压趋于平稳。2年来患者出现记忆力减退，近事遗忘，半月来明显加重，夜间睡眠差，经常夜间起来烧饭等异常行为于2023年5月13日在松江区精神卫生中心就诊，诊断为脑损害和功能障碍及躯体疾病引起的精神障碍。给予奥氮平片口服后稍有好转。因患者年事渐高，生活不能完全自理，今入住我福利院。发病以来，患者无发热，无呕吐腹泻，无胸闷气急等，饮食可，大小便无异常。患者因冠心病、脂肪肝目前服用硫酸氢氯吡格雷和阿托伐他汀钙片。体检：体温 36.8℃，脉搏76次/分，呼吸20次/分，血压125/71mmHg。两肺呼吸音粗，心率76次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：9.17mmol/L。糖化血红蛋白：8.8%。心电图：窦性心律，一度房室传导阻滞，电轴左偏异常Q波。B超：脂肪肝、肝囊肿，胆囊、脾脏、双肾未见明显占位，胰腺显示不清。CT：右枕叶软化灶，老年脑改变，慢性支气管炎病变，左肺下叶少量炎症，心影增大，主动脉及冠状动脉硬化(2023.5.16松江区中心医院)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'556', N'1279', N'患者家属', N'基本可信', N'庄秋丽', N'2023-06-01 00:00:00.000', NULL, NULL, NULL, NULL, N'郁卫祖，女，74岁，进行性认知功能障碍7-8年。患者10余年前因车祸引起颅脑骨折，颅脑骨折后经常出现癫痫样症状。7-8年前骑自行车摔倒再次引起颅脑骨折，及左侧锁骨骨折，之后2-3年出现渐进性认知能力障碍，两手臂抬举困难，目前患者思维功能贫乏，自知力差，认知能力障碍，反应迟钝。
2-3年前因患者认知障碍，平时常坐立不定，曾于松江精神卫生中心就诊，CT检查：“脑萎缩”，诊断：“血管性痴呆”，而予以盐酸舍曲林片治疗。
本次入院前检查，(5月16-17日中心医院）血生化提示：高脂血症。B超：脂肪肝。CT:双侧额叶及颞叶对称性低密度影，水肿？萎缩？双侧基底节区及侧脑室旁腔隙灶老年脑改变。双肺少许炎症，右肺下叶小结节，腰1椎体压缩性骨折。
患者目前一般情况可，无发热，纳食可，无咳嗽咳痰等，两便无异常，两下肢行走欠稳（1-2年前开始出现）。
体检：体温 36.2℃，脉搏66次/分，呼吸20次/分，血压138/86mmHg。神清，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，未及明显干湿啰音，心率66次/分，心律齐，腹软，全腹无压痛，无反跳痛，肝脾肋下未及肿大，四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为4级，左下肢肌力为4级，右上肢肌力为4级，右下肢肌力为4级。
生化:葡萄糖：4.87mmol/L。Rbc：4.22×10<sup>12</sup>/L，Hb：131g/L，Wbc：5.69×10<sup>9</sup>/L，中性粒比例：56.30%，淋巴细胞比例：33.60%，Plt：206×10<sup>9</sup>/L。C反应蛋白：8.25MG/L，糖化血红蛋白：5.30%，低密度脂蛋白：4.79mmol/L，甘油三酯：1.78mmol/L，高密度脂蛋白：1.88mmol/L，总胆固醇：7.32mmol/L。Ca：2.25mmol/L，Cl：102.27mmol/L，K：3.81mmol/L，Na：142.12mmol/L，P：1.45mmol/L。A/G：1.35，白蛋白：40.16g/L，谷丙转氨酶：10.09u/L，间接胆红素：4.76umol/L，碱性磷酸酶（ALP）：107.16u/L，总胆红素：8.09umol/L，总蛋白：69.84g/L。球蛋白：29.68g/L。直接胆红素：3.33umol/L。肌酐：51.41umol/L，尿素氮：2.56mmol/L，尿酸：287.94umol/L，心电图：窦性心律，T波改变，V3-V6浅倒置。B超：脂肪肝，胆囊、胰腺、脾脏、双肾未见明显占位。CT：双侧额叶及颞叶对称性低密度影，水肿？萎缩？双侧基底节区及侧脑室旁腔隙灶老年脑改变。双肺少许炎症，右肺下叶小结节，腰1椎体压缩性骨折。
')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'558', N'1281', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2023-06-07 00:00:00.000', NULL, NULL, N'', N'', N'黄雪勤，女，79岁，左肩部左髋部外伤后生活不能自理八月余入院。患者于2022年9月18日不慎摔倒致左肩部左髋部肿胀疼痛伴活动受限在松江区中心医院住院，行左肱骨骨折切开复位内固定术+骨盆外固定术后9月23日出院。患者因生活不能自理今入住我福利院，入院体检提示房性早搏、脂肪肝、肝囊肿、脑梗死等。发病以来患者无发热，无胸闷气急，无咳嗽，无头痛呕吐等不适，饮食睡眠尚可。发现脑梗死两年余，未予治疗。体检：体温 36.7℃，脉搏86次/分，呼吸20次/分，血压142/86mmHg。两肺呼吸音粗，心率86次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：5.9mmol/L。糖化血红蛋白：6%，低密度脂蛋白：3.46mmol/L，甘油三酯：1.85mmol/L，高密度脂蛋白：1.11mmol/L，总胆固醇：5.01mmol/L，碱性磷酸酶（ALP）：145.97u/L，肌酐：85.6umol/L，尿素氮：6.14mmol/L，尿酸：304.78umol/L。尿常规 白细胞：4u/L隐血：+。心电图：窦性心律，房性早搏。X线胸片：左侧耻骨上下支骨折后，左肱骨上段骨折术后，左肘关节部分融合改变。B超：脂肪肝，肝囊肿，胆囊、胰腺、脾脏、双肾未见明显占位。CT：心脏增大，主动脉及冠状动脉硬化，双侧基底节区腔隙灶，右顶叶软化灶，老年脑改变（2023.6.2松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'559', N'1282', N'患者本人及家属', N'可靠', N'袁纯兰', N'2023-06-20 00:00:00.000', NULL, NULL, N'', N'', N'赵全娟，女，86岁，时有头晕头痛20余年入院。患者20年前无明显诱因下出现头晕头痛，在松江中心医院就诊，测血压160/95mmhg，诊断为“高血压病”，给予氨氯地平片口服后血压控制可。后因双下肢肿胀于两年前调整为氯沙坦钾氢氯噻嗪片口服，目前患者血压在正常范围，双下肢肿胀好转。因年事渐高，家中无人照顾，今入住我福利院。本次入院体检提示高脂血症、高尿酸血症、脂肪肝、骨质疏松症等，发病以来，患者无发热，无胸闷气急，无恶心呕吐，精神、饮食可，安眠药助眠，大小便正常。体检：体温 36.8℃，脉搏80次/分，呼吸20次/分，血压128/77mmHg。两肺呼吸音清，心率80次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：5.16mmol/L。糖化血红蛋白：5.6%，甘油三酯：2.6mmol/L，总胆固醇：6.04mmol/L。肌酐：112.55umol/L，尿素氮：6.08mmol/L，尿酸：535.23umol/L。尿常规 白细胞：1117u/L，葡萄糖：（-）mmol/L，隐血：+。心电图：正常心电图X线胸片：双膝关节退行性改变。B超：脂肪肝、肝脏多发囊肿，右肾囊肿，左肾钙质沉着，胆囊术后未显示，胆总管代偿性扩张，胰腺、脾脏未见明显占位。CT：慢性支气管炎改变，主动脉及冠脉硬化，老年脑改变，右侧放射冠小斑片状钙化灶(2023.6.13松江区中心医院)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'574', N'1297', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2023-09-22 00:00:00.000', NULL, NULL, N'', N'', N'朱寿田，男，83岁，时有头晕头痛15年余入院。既往高血压病、糖尿病、冠心病、起搏器植入后、脑梗死、脂肪肝、胆囊炎、胆囊结石、前列腺增生、高尿酸血症、脂肪肝、左肾囊肿、右侧肾盂扩张等。体检：体温 37.1℃，脉搏88次/分，呼吸20次/分，血压135/71mmHg。两肺呼吸音清，心率88次/分，心律齐，腹软，无压痛。四肢无水肿。生化:葡萄糖：4.77mmol/L。Rbc：4.35×10<sup>12</sup>/L，Hb：136g/L，Wbc：7.47×10<sup>9</sup>/L，中性粒比例：58.5%，淋巴细胞比例：25.6%，Plt：165×10<sup>9</sup>/L。糖化血红蛋白：7.3%，低密度脂蛋白：1.22mmol/L，甘油三酯：1.66mmol/L，高密度脂蛋白：0.91mmol/L，总胆固醇：2.59mmol/L，Cl：103.87mmol/L，K：4.72mmol/L，Na：140.17mmol/L，A/G：2.03，白蛋白：43.91g/L，谷丙转氨酶：10.74u/L，间接胆红素：9.82umol/L，碱性磷酸酶（ALP）：52.57u/L，总胆红素：16.4umol/L，总蛋白：65.59g/L。球蛋白：21.68g/L。直接胆红素：6.58umol/L。肌酐：189.87umol/L，尿素氮：9.53mmol/L，尿酸：516umol/L。心电图：窦性心律，起搏节律。CT：双侧放射冠区腔隙灶，老年脑改变，脂肪肝，慢性胆囊炎，胆囊结石，右肾肾盂稍扩张，左肾囊肿，胰腺、脾脏未见明显占位。右肺上叶及下叶小结节，右肺中叶钙化灶，心脏增大，主动脉硬化，心脏起搏器置入，胆囊多发结石，左肾囊性灶（2023.9.8上海市松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'576', N'1300', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2023-11-01 00:00:00.000', NULL, NULL, N'', N'', N'杨桂芳，女，92岁，反复头晕头痛15余年。患者15余年来反复出现头晕头痛，无视物旋转，无黑朦，无恶心呕吐，无胸闷胸痛，无大汗淋漓，于松江区中心医院就诊，诊断为“高血压病”予对症治疗，具体药物不详，头晕头痛好转。监测血压略偏高，数值不详。目前患者偶有头晕头痛，长期口服坎地沙坦酯片，发病以来患者无恶心呕吐，无大汗淋漓，无胸闷胸痛，无发热，胃纳睡眠可，二便无异常。
患者因冠状动脉粥样硬化性心脏病、慢性胃炎、肾功能不全、贫血、脑供血不足、高尿酸血症、帕金森、腹水长期口服麝香保心丸、酒石酸美托洛尔片、复方α-酮酸片、琥珀酸亚铁缓释片、甲磺酸倍他司汀片、非布司他片、苯海索、叶酸片、托拉塞米。体检：体温 36.9℃，脉搏65次/分，呼吸20次/分，血压145/67mmHg。两肺呼吸音清，心率65次/分，心律齐，腹软，无明显压痛，无液波震颤。生化:葡萄糖：4.87mmol/L。糖化血红蛋白：5%，低密度脂蛋白：1.56mmol/L，甘油三酯：0.60mmol/L，高密度脂蛋白：1.73mmol/L，总胆固醇：3.38mmol/L。A/G：0.87，白蛋白：28.47g/L，谷丙转氨酶：17.11u/L，间接胆红素：2.88umol/L，碱性磷酸酶（ALP）：101.61u/L，总胆红素：6.03umol/L，总蛋白：61.37g/L。球蛋白：32.9g/L。直接胆红素：3.15umol/L。肌酐：153.33umol/L，尿素氮：11.84mmol/L，尿酸：486.36umol/L，白细胞：24u/L，尿胆原：阴性，葡萄糖：阴性mmol/L，酸碱度：5.0，酮体：阴性，亚硝酸盐：阴性，隐血：++。心电图：窦性心律 T波改变 II III avF V3-V6低平或浅倒置。B超：血吸虫性肝硬化、胆囊壁毛糙增厚（继发性改变）、右肾囊肿、左肾钙质沉着、胰腺、脾脏未见明显占位。附见：腹腔积液。CT：胸部CT：1.心脏增大，主动脉、冠状动脉硬化，少量心包积液。2.两侧少量胸腔积液。附见：肝硬化，腹腔积液，腹壁皮下水肿。头颅CT；老年脑改变，建议随访或MRI检查，（2023.10.24松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'577', N'1301', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2023-11-06 00:00:00.000', NULL, NULL, N'', N'', N'陈华秀，女，79岁，记忆力减退、猜疑东西被偷4年余。患者约2019年开始出现记忆力减退，记不住新近发生的事情，老说陈年往事伴猜疑，怀疑东西被人偷走，经常乱翻东西。于2022年1月24日在上海市第一人民医院南院就诊，CT检查提示脑萎缩，脑白质病。诊断为老年痴呆。予以盐酸多奈哌齐平、石杉碱甲、尼莫地平等治疗，后定期在松江区中心医院配药治疗，目前患者服用盐酸多奈哌齐平、石杉碱甲治疗。发病以来，患者无头晕头痛，无发热，无呕吐，无明显消瘦等，饮食睡眠可。
患者因心律失常、蛋白尿等目前服用肾炎康复片、氯沙坦钾片、稳心颗粒，冠心舒胶囊等。否认高血压病、糖尿病等，右手臂外伤骨折后十余年。体检：体温 37℃，脉搏63次/分，呼吸20次/分，血压175/95mmHg。两肺呼吸音清，心率63次/分，心律齐，腹软，无压痛。生化:葡萄糖：4.9mmol/L，Rbc：4.11×10<sup>12</sup>/L，Hb：116g/L，Wbc：6.17×10<sup>9</sup>/L，中性粒比例：71.3%，淋巴细胞比例：17.5%，Plt：19610<sup>9</sup>/L。糖化血红蛋白：5.8%，低密度脂蛋白：2.79mmol/L，甘油三酯：0.8mmol/L，高密度脂蛋白：1.69mmol/L，总胆固醇：4.68mmol/L。A/G：1.29，白蛋白：38.02g/L，谷丙转氨酶：13.42u/L，间接胆红素：4.09umol/L，碱性磷酸酶（ALP）：127.67u/L，总胆红素：6.9umol/L，总蛋白：67.52g/L。球蛋白：29.5g/L。直接胆红素：2.81umol/L。肌酐：71.6umol/L，尿素氮：5.82mmol/L，尿酸：263.97umol/L，白细胞：10u/L，尿胆原：阴性，葡萄糖：阴性mmol/L，酸碱度：6，酮体：阴性，亚硝酸盐：阴性，隐血：阴性。心电图：窦性心律 左室高电压 房性早搏 ST改变 V5 V6水平型压低0.05-0.10mv T波改变 I II III avF V3-V6低平、双相或浅倒置。B超：肝脏囊肿 左肾囊肿 胆囊、胰腺、脾脏、右肾未见明显占位。CT：胸部CT:心脏增大，主动脉及冠状动脉硬化 附见：肝脏多发囊性灶。头颅CT：老年脑改变，请结合MR检查。（2023.10.10 松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'580', N'1303', N'患者本人及家属', N'基本可信', N'庄秋丽', N'2024-01-04 00:00:00.000', NULL, NULL, NULL, NULL, N'卫建青，男，84岁，慢性咳嗽咳痰60年左右。患者原有慢性支气管炎史60年左右，反复急性发作，长期慢性咳嗽咳痰，伴有气喘，在家长期吸氧，稍作活动气喘加剧，目前稍有咳嗽咳痰，痰呈白粘痰，尚能咳出，气稍促，能平卧，无咯血及胸痛等现象。
患者原有高血压病史20年左右，以往最高血压150/100mmHg，平时无头晕头痛等不适，长期服用厄贝沙坦氢氯噻嗪片降压药，据说血压控制可。
患者7-8年前因手指无感觉于中心医院就诊，经CT检查“脑梗死”，经活血通络等治疗后病情好转，无后遗症。之后几年类似症状又先后发作2次，均于中心医院就诊，用药后好转，无后遗症。
患者去年7-8月份于中心医院就诊检查时发现肾功能不全，无相应不适症状，本次入本院前体检提示肌酐、尿素氮、尿酸高。
本次入院前检查：CT：双侧基底节——放射冠区多发腔隙灶，老年脑改变。左肺上叶纤维灶。主动脉及冠状动脉硬化。B超：右肾结石。
患者目前一般情况尚可，无发热，咳嗽咳痰，痰呈白粘痰，尚能咳出，稍气促，能平卧，纳食可，小便无异常，大便常难解需用通便药通便，夜间睡眠可。
体检：体温 36.8℃，脉搏89次/分，呼吸26次/分，血压124/79mmHg。神清，呼吸稍急促，唇不绀，两肺呼吸音粗，无明显干湿啰音，心率89次/分，律不齐，闻及3－5次/分早搏，腹软，全腹无压痛，肝脾肋下未及肿大，四肢无水肿。生化:葡萄糖：7.74mmol/L。Rbc：4.89×10<sup>12</sup>/L，Hb：151g/L，Wbc：6.29×10<sup>9</sup>/L，中性粒比例：51.50%，淋巴细胞比例：34.10%，Plt：232×10<sup>9</sup>/L。糖化血红蛋白：6%，低密度脂蛋白：5.06mmol/L，甘油三酯：1.41mmol/L，高密度脂蛋白：1.50mmol/L，总胆固醇：6.96mmol/L。Ca：2.46mmol/L，Cl：99.60mmol/L，K：4.56mmol/L，Na：138mmol/L，P：0.88mmol/L。A/G：1.83，白蛋白：46.10g/L，谷丙转氨酶：15.20u/L，间接胆红素：12.05umol/L，碱性磷酸酶（ALP）：63.10u/L，总胆红素：16.30umol/L，总蛋白：71.28g/L。球蛋白：25.18g/L。直接胆红素：4.25umol/L。肌酐：154umol/L，尿素氮：8mmol/L，尿酸：425umol/L。心电图：窦性心律，房性早搏，P-R间期正常高限，T波变化I avL V5V6低平。B超：右肾结石，肝脏、胆囊、胰腺、脾脏、左肾未见明显异常。CT：双侧基底节——放射冠区多发腔隙灶，老年脑改变。左肺上叶纤维灶。主动脉及冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'575', N'1299', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2023-10-23 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'581', N'1305', N'患者家属', N'基本可靠', N'胡新志', N'2024-01-05 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'582', N'1304', N'患者家属', N'可信', N'王屹', N'2024-01-05 00:00:00.000', NULL, NULL, NULL, NULL, N'王晓东，男，75岁，左侧肢体活动不利六年。患者六年前突发左侧肢体活动不利伴口齿不清，由家属送至松江区中心诊治，诊断“脑出血”，予住院治疗后病情稳定。患者平时口服阿托伐他汀钙片、阿司匹林肠溶片、胞磷胆碱钠片等治疗，目前日常生活功能障碍，运动功能障碍。今由家属送入本院住养。患者现精神状态可，坐轮椅，食欲、睡眠大小便较正常。患者因高血压史多年，予厄贝沙坦氢氯噻嗪片、福马酸比索洛尔片等治疗；因糖尿病多年予阿卡波糖片治疗。前列腺增生三年。体检：体温 36.5℃，脉搏79次/分，呼吸18次/分，血压170/90mmHg。伸舌右偏。两肺呼吸音清，79次/分，心律齐，无腹壁紧张，无压痛，四肢无水肿，左上肢肌力为3级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。左指关节呈内收状。生化:葡萄糖：18.97mmol/L。Rbc：5.27×10<sup>12</sup>/L，Hb：163g/L，Wbc：6.30×10<sup>9</sup>/L，中性粒比例：79.90%，淋巴细胞比例：20.0%，Plt：156×10<sup>9</sup>/L。糖化血红蛋白：10.90%，低密度脂蛋白：2.69mmol/L，甘油三酯：1.6mmol/L，高密度脂蛋白：0.96mmol/L，总胆固醇：4.05mmol/L。Ca：2.48mmol/L，Cl：91.40mmol/L，K：4.24mmol/L，Na：125.0mmol/L，P：1.29mmol/L。A/G：1.54，白蛋白：47.40g/L，谷丙转氨酶：18.20u/L，间接胆红素：13.74umol/L，碱性磷酸酶（ALP）：63.90u/L，总胆红素：20.21umol/L，总蛋白：78.12g/L。球蛋白：30.72g/L。直接胆红素：6.47umol/L。肌酐：96.10umol/L，尿素氮：6.34mmol/L，尿酸：424.10umol/L，白细胞：0.50u/L，管型：0.13u/L，尿胆原：（-），葡萄糖：++++mmol/L，酸碱度：5.5，酮体：（-），上皮细胞：2.6u/L，亚硝酸盐：（-），隐血：（-）。心电图：窦性心律X线胸片：(缺)。B超：左肾囊肿CT：右侧基底节区、丘脑区脑出血恢复期改变。老年脑改变。主动脉、冠状动脉硬化')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'586', N'1309', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-01-25 00:00:00.000', NULL, NULL, N'', N'', N'陈小洪，男，89岁，记忆力减退6年。患者6年来无明显诱因下出现记忆力减退，不知道自己年龄，不知道时间、地点，不分白天黑夜，不认识自己儿媳妇，找不到厕所等。曾经在上海市第一人民医院南院就诊，诊断为：老年痴呆症。未予药物治疗。因生活不能完全自理，现入住我福利院。发病以来，患者无发热，无头晕头痛，无胸闷气急，无呕吐腹泻等。
本次入院体检提示糖尿病，予以德谷门冬胰岛素注射液10iubid皮下注射。脑梗死、高脂血症，外院未予治疗。左肩部脓肿引流术后目前外院定期换药。体检：体温 36.6℃，脉搏94次/分，呼吸20次/分，血压122/79mmHg。两肺呼吸音清，心率94次/分，心律齐，腹软，无压痛。生化:葡萄糖：20mmol/L，糖化血红蛋白：11.1%，低密度脂蛋白：3.1mmol/L，甘油三酯：2.1mmol/L，高密度脂蛋白：0.61mmol/L，总胆固醇：4.2mmol/L，Cl：94.9mmol/L，K：4.09mmol/L，Na：133mmol/L，A/G：1.3，白蛋白：34.4g/L，谷丙转氨酶：25u/L，碱性磷酸酶（ALP）：77u/L，总胆红素：10.2umol/L，总蛋白：60.2g/L，球蛋白：25.8g/L。直接胆红素：3.9umol/L。肌酐：83.4umol/L，尿素氮：7.3mmol/L，尿酸：292umol/L，尿胆原：+，尿葡萄糖：++++mmol/L，心电图：窦性心律，左前分支传导阻滞。B超：脂肪肝，肝囊肿，胆囊壁稍毛糙。CT：双侧基底节区腔隙灶，脑萎缩，脑白质病，颅内动脉硬化，双肺通气不均，主动脉及冠状动脉硬化，心包少量积液，甲状腺密度不均匀，肝内低密度影（上海市第一人民医院2024.1.9）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'588', N'1311', N'患者本人及家属', N'基本可信', N'庄秋丽', N'2024-02-16 00:00:00.000', NULL, NULL, NULL, NULL, N'殷德兆，男，85岁，反复高血压30年。患者原有原发性高血压病30年，因服药不规律，故血压控制不够理想，最高血压160/90mmHg，平时无头晕头痛等不适。
患者2012年12月曾脑溢血，当时于上海普陀区中心医院住院治疗，目前已基本恢复健康，只落下口齿不清后遗症。
患者两膝关节反复酸痛，出现行走不利现象3-4年，曾于上海普陀区中心医院CT检查，提示“骨刺”。
患者目前一般情况可，无发热，纳食可，小便无异常，大便难解需用通便药通便，夜眠可。
体检：体温 36.6℃，脉搏71次/分，呼吸20次/分，血压143/87mmHg。神清，精神可，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率71次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢无水肿，各关节活动可。生化:葡萄糖：5.7mmol/L。Rbc：5.3×10<sup>12</sup>/L，Hb：159g/L，Wbc：5.3×10<sup>9</sup>/L，中性粒比例：60.3%，淋巴细胞比例：32.3%，Plt：198×10<sup>9</sup>/L。白蛋白：47.1g/L，谷丙转氨酶：15.5u/L，碱性磷酸酶（ALP）：85.4u/L，总胆红素：21.5umol/L，总蛋白：84g/L。直接胆红素：6.4umol/L。肌酐：90.8umol/L，尿素氮：4.98mmol/L，尿酸：389.8umol/L，CT：右肺下叶少许炎性纤维灶，左肺上叶磨玻璃结节，冠脉壁部分钙化。肝内小囊性灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'589', N'1312', N'患者家属', N'基本可信', N'庄秋丽', N'2024-02-21 00:00:00.000', NULL, NULL, NULL, NULL, N'朱家林，男，67岁，主动脉支架植入术后3年。患者2020年12月25日因胸背痛经上海第九人民医院CTA检查确诊为：胸腹主动脉瘤，2021年2月9日于上海中山医院行胸主动脉分支覆膜支架置入+颈总动脉—锁骨下动脉搭桥术。2022年3月于上海一院再次行腹主动脉支架植入术，术后出现失语，自知力认知能力下降，右侧肢体瘫痪后遗症。
患者原有原发性高血压病15年，最高血压220/95mmHg，长期服用降压药，控制不够理想。
患者10年前曾出现脑梗塞，入院前体检CT：左侧额颞顶叶大片软化灶，右侧丘脑区陈旧性腔隙灶。
患者患有前列腺增生2年。
入院前体检：血生化检查提示低钾血症、肾功能不全。B超提示：双肾弥漫性病变。
患者目前一般情况可，无发热，纳食可，小便无异常，大便需用通便药通便，夜间睡眠可。
体检：体温 36.4℃，脉搏67次/分，呼吸20次/分，血压122/87mmHg。神清，精神可，呼吸平稳，T36.4°C，BP122/87mmHg，氧饱和度98%，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率67次/分，律齐，腹软，全腹无压痛，肝脾肋下未及肿大，肠鸣音无亢进，两下肢无水肿，左侧肢体肌力可，右侧肢体肌力0级。
生化:葡萄糖：5.93mmol/L。Rbc：4.39×10<sup>12</sup>/L，Hb：127g/L，Wbc：8.53×10<sup>9</sup>/L，中性粒比例：68%，淋巴细胞比例：24%，Plt：306×10<sup>9</sup>/L。糖化血红蛋白：5.70%，低密度脂蛋白：2.57mmol/L，甘油三酯：1.87mmol/L，高密度脂蛋白：0.93mmol/L，总胆固醇：3.93mmol/L。Ca：2.20mmol/L，Cl：97.44mmol/L，K：3.11mmol/L，Na：139.54mmol/L，P：1.13mmol/L。谷丙转氨酶：16.98u/L，间接胆红素：4.40umol/L，总胆红素：6.85umol/L，直接胆红素：2.45umol/L。肌酐：156.33umol/L，白细胞：7.00u/L，管型：0.00u/L，尿胆原：阴性（-），葡萄糖：阴性mmol/L（-），酸碱度：7.0，酮体：阴性（-），上皮细胞：1.00u/L，亚硝酸盐：阴性（-），隐血：阴性（-）。心电图：窦性心律，电轴左偏，I度房室传导阻滞，异常Q波，ST——T改变，V1-V3上斜型抬高0.05-0.10mv，I AVL V4-V6水平型压低0.05-0.10mv，I avL v4-v6压低倒置。B超：双肾弥漫性病变、双肾囊肿，肝、脾、双肾未见明显破裂征象，腹腔未见明显积液。CT：左侧额颞顶叶大片软化灶，右侧丘脑区陈旧性腔隙灶。慢性支气管炎、肺气肿，两肺下叶部分间质性纤维灶，心脏增大，主动脉、冠状动脉硬化，少量心包积液，主动脉支架植入中。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'591', N'1314', N'患者家属', N'基本可靠', N'胡新志', N'2024-03-22 00:00:00.000', NULL, NULL, N'', N'', N'蒋五妹，女，61岁，左侧肢体活动不利一年余。患者十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服缬沙坦胶囊，因服药不规律，血压控制不稳定，最高时可达170/110mmHg，未引起重视。于2023年2月11日突然意识不清、昏迷在家中2天，大小便失禁，家人发现后即送松江中心医院救治，诊断：“脑溢血”。住院治疗好转出院，遗留左侧肢体活动不利，借助助步器行走10米。现因个人生活不能自理，于20240322自愿入住我院。患者一般情况尚可，轮椅推入病室，简单对答，查体合作，饮食可，睡眠尚可，大小便正常。否认肝炎、结核等传染病史。体检：体温 36.4℃，脉搏82次/分，呼吸20次/分，血压130/85mmHg。两肺呼吸音清，82次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为3级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。左下肢踝关节骨折手术史。生化:葡萄糖：5.15mmol/L糖化血红蛋白：5.4%，低密度脂蛋白：2.11mmol/L，甘油三酯：1.76mmol/L，高密度脂蛋白：0.97mmol/L，总胆固醇：3.55mmol/L。Cl：102.38mmol/L，K：4.28mmol/L，Na：144.52mmol/L，P：1.39mmol/L，A/G：1.37，白蛋白：42.95g/L，谷丙转氨酶：7.93u/L，总蛋白：74.23g/L，肌酐：68.38umol/L，尿素氮：2.96mmol/L，尿酸：545.97umol/L，心电图：正常心电图。B超：脂肪肝、右肾钙乳症、左肾小囊肿、胆囊、胰腺、脾脏未见明显占位，CT：头颅CT：1.右侧丘脑脑出血改变、趋软化灶2.双侧放射冠区、基底节区腔隙灶3.老年脑、脑白质变性')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'594', N'1317', N'患者家属', N'可靠', N'袁纯兰', N'2024-03-29 00:00:00.000', NULL, NULL, N'', N'', N'倪惠福，男，84岁，记忆力下降约9年，加重3年。患者约2015年开始出现记忆力下降，易忘事，2021年开始加重，不认识自己的妻子和儿女，不知道吃饭穿衣，外出后不认识回家的路，曾走丢四次，在家人和民警协助下回家。2021年后在松江中心医院就诊，诊断为睡眠障碍、阿尔茨海默病、认知障碍、慢性缺血性脑血管病予以多奈哌齐、胞磷胆碱钠、吡拉西坦、银杏叶片、艾司唑仑等口服。病情未见明显好转。因生活不能自理，今入住我院，发病以来，患者无慢性发热，无头痛呕吐，无胸闷气急等，饮食可，睡眠一般。50余年前脑外伤病史。否认高血压、糖尿病等。体检：体温 36.4℃，脉搏66次/分，呼吸20次/分，血压141/73mmHg。两肺呼吸音清，心率80次/分，心律齐，腹软，无压痛。生化:葡萄糖：4.87mmol/L；Rbc：4.12×10<sup>12</sup>/L，Hb：133g/L，Wbc：5.97×10<sup>9</sup>/L，中性粒比例：74.7%，淋巴细胞比例：16.8%，Plt：179×10<sup>9</sup>/L；糖化血红蛋白：5.5%，低密度脂蛋白：3.31mmol/L，甘油三酯：0.63mmol/L，高密度脂蛋白：1.38mmol/L，总胆固醇：4.61mmol/L；Cl：102.65mmol/L；K：3.78mmol/L，Na：141.2mmol/L，A/G：1.49，白蛋白：42.68g/L，谷丙转氨酶：9.85u/L，间接胆红素：14.43umol/L，碱性磷酸酶（ALP）：94.91u/L，总胆红素：22.31umol/L（2024.3.26松江区中心医院），总蛋白：71.37g/L。球蛋白：28.69g/L。直接胆红素：7.88umol/L。肌酐：108.35umol/L，尿素氮：6.57mmol/L，尿酸：339.35umol/L；心电图：窦性心律，I度房室传导阻滞；B超：肝脏弥漫性病变（血吸虫肝病可能），肝脏囊肿，右肾结石，左肾钙乳症，胆囊、胰腺、脾脏未见明显占位；CT：两肺上叶纤维增殖灶，双侧胸膜稍增厚。主动脉硬化，老年脑改变（2024.3.26松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'595', N'1318', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2024-04-07 00:00:00.000', NULL, NULL, N'', N'', N'蒋全英，女，80岁，记忆力减退约10年，加重半年。患者10年前无明显诱因下出现一侧肢体乏力，在松江区中心医院就诊诊断为脑梗死，予以治疗后乏力缓解。后患者出现记忆力减退，记不住新近发生的事情，经常提起很久以前的事情，忘记关煤气，吃饭时间也会忘记，药吃过了还会再吃。因生活自理能力下降今入住我福利院。发病以来患者无咳嗽，无发热，无腹痛呕吐，无明显冲动、伤人等行为。饮食可，夜眠差服用阿普唑仑后睡眠改善。
患者既往高血压病、冠心病、房颤、糖尿病等，目前服用甲磺酸艾多沙斑、普伐他汀、麝香保心丸、托拉塞米、达格列净、芪苈强心胶囊、氨氯地平贝那普利片等。经常性活动后气促，血压控制尚可，血糖控制不满意。体检：体温 36.5℃，脉搏70次/分，呼吸20次/分，血压133/72mmHg。两肺呼吸音清，心率70次/分，心律不齐，腹软，无压痛，无反跳痛，无液波震颤，无肿块。双下肢膝以下凹陷性水肿。生化:葡萄糖：12.62mmol/L；糖化血红蛋白：5.8%；低密度脂蛋白：2.37mmol/L，甘油三酯：0.6mmol/L，高密度脂蛋白：1.41mmol/L，总胆固醇：3.83mmol/L；Cl：102.22mmol/L，K：3.57mmol/L，Na：140.89mmol/L，A/G：1.88，白蛋白：45.3g/L，谷丙转氨酶：8.15u/L，间接胆红素：6.73umol/L，碱性磷酸酶（ALP）：99.12u/L，总胆红素：11.92umol/L，总蛋白：69.4g/L；直接胆红素：5.19umol/L。肌酐：95.34umol/L，尿素氮：6.93mmol/L。心电图：心房颤动、左室高电压（2024.3.13松江区中心医院）。B超：肝脏、胆囊、胰腺、脾脏、双肾未见明显占位（2024.3.13松江区中心医院）CT：右侧后分水岭区脑软化灶，左侧丘脑及两侧放射冠区多发腔隙灶，脑白质变性，老年脑改变，心脏明显增大，主动脉及冠状动脉硬化，左肺上叶纤维小钙化灶，左侧胸腔少量积液（2024.3.13松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'597', N'1320', N'患者家属', N'可信', N'王屹', N'2024-04-12 00:00:00.000', NULL, NULL, NULL, NULL, N'朱水林，男，78岁，口齿不清伴记忆力减退一年。患者一年前无明显诱因下突发口齿不清反应迟钝，即由家属送至松江区中心医院诊治，诊断：脑梗死，治疗后症状基本稳定，平时予吡拉西坦片、阿司匹林肠溶片、阿托伐他汀钙片等治疗。近期患者反应迟钝记忆力下降明显，交流沟通减少，行动缓慢，生活自理能力减退，由家属送至本院住养。患者原有高血压史多年，长期服苯磺酸氨氯地平片，血压控制尚可（具体不详）。患者本次入院体检提示脂肪肝、胆囊息肉。患者目前精神状态尚可，表情淡漠，纳可，大小便正常。冠心病、慢性支气管炎一年余。体检：体温 36.6℃，脉搏65次/分，呼吸18次/分，血压146/84mmHg。两肺呼吸音粗，心率65次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.28mmol/L。Rbc：4.23×10<sup>12</sup>/L，Hb：129g/L，Wbc：7.99×10<sup>9</sup>/L，中性粒比例：43.2%，淋巴细胞比例：44.6%，Plt：310
×10<sup>9</sup>/L（2024.4.2松江区中心医院）。糖化血红蛋白：5.80%，低密度脂蛋白：1.36
mmol/L（2.24.3.30松江区中心医院），甘油三酯：1.19mmol/L，高密度脂蛋白：1.19mmol/L，总胆固醇：2.89mmol/L。Ca：2.42mmol/L，Cl：100.81mmol/L，K：3.98mmol/L，Na：138.18mmol/L，P：1.21
mmol/L（2024.3.30松江区中心医院）。A/G：1.48，白蛋白：44.53g/L，谷丙转氨酶：16.08u/L，间接胆红素：6.74umol/L，碱性磷酸酶（ALP）：113.60
u/L（2024.3.30松江区中心医院），总胆红素：11.21umol/L，总蛋白：74.67g/L。球蛋白：30.14g/L。直接胆红素：4.47umol/L。肌酐：81.9umol/L，尿素氮：6.48mmol/L，尿酸：321.65
umol/L（2024.3.30松江区中心医院），心电图：一度房室传导阻滞X线胸片：(缺)。B超：脂肪肝、胆囊息肉CT：头部CT：1、右侧额颞岛叶软化灶，脑内多发腔隙灶2、老年脑，脑白质变性
胸部CT:1、右肺下叶纤维灶2、主动脉及冠状动脉硬化。心包少量积液。（2024.3.31松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'598', N'1321', N'患者家属', N'基本可靠', N'胡新志', N'2024-04-15 00:00:00.000', NULL, NULL, N'', N'', N'郁秀英，女，93岁，反复头晕头痛10年。患者十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服硝苯地平控释片，血压控制尚稳定。5年前开始出现活动后胸闷气促明显，无呼吸困难，夜间无高枕卧位，在松江中心医院诊断“冠心病、心功能不全”予以口服药治疗（具体治疗不详），病情稍有好转，双下肢长期浮肿。现因个人生活不能自理，于20240415自愿入住我院。患者一般情况尚可，轮椅推入病室，简单对答，查体合作，饮食可，睡眠尚可，大小便正常，双下肢凹陷性水肿明显。否认肝炎、结核等传染病史。体检：体温 36.7℃，脉搏74次/分，呼吸20次/分，血压118/64mmHg。两肺呼吸音清，心率74次/分，心律齐，无腹壁紧张，无压痛，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。双下肢凹陷性水肿，右股骨颈骨折史。生化:葡萄糖：5.14mmol/L。低密度脂蛋白：3.40mmol/L甘油三酯：0.78mmol/L，高密度脂蛋白：1.74mmol/L，总胆固醇：5.37mmol/L。Ca：2.21mmol/L，Cl：102.08mmol/L，K：4.82mmol/L，Na：138.9mmol/L，P：1.21mmol/LA/G：1.72，白蛋白：42.26g/L，谷丙转氨酶：12.98u/L，间接胆红素：5.71umol/L，碱性磷酸酶（ALP）：79.15u/L总胆红素：10.41umol/L，总蛋白：66.86g/L。球蛋白：24.60g/L。直接胆红素：4.70umol/L。肌酐：66.29umol/L，尿素氮：7.95mmol/L，尿酸：321.06umol/L心电图：ST-T波改变 ST I avL II III avF V4-V6水平型压低B超：血吸虫肝病、胆囊、胰腺、脾脏、双肾未见明显占位，CT：胸部CT:1.两肺散在少许炎症改变2.右肺上叶小结节3.心脏增大、主动脉冠状动脉硬化4.食管裂孔疝。头颅CT:两侧基底节区及右侧放射冠区多发腔隙灶，老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'604', N'1328', N'患者本人及家属', N'基本可靠', N'胡新志', N'2024-05-08 00:00:00.000', NULL, NULL, N'', N'', N'陆元娥，女，81岁，反复头晕头痛6个月。患者半年前无诱因出现反复头晕头痛，双下肢乏力不适，无恶心呕吐，无肢体功能障碍，未引起重视，未就诊。4月底因入住本院在松江中心医院进行入院体检，监测血压180/110mmHg，诊断“高血压、腔隙性脑梗死”即予以口服拉西地平片、丹参片，血压控制尚稳定维持在120-140/70-90mmHg之间。现因年事已高、个人生活自理差，于20240508自愿入住我院。患者一般情况尚可，扶入病室，对答切题，查体合作，饮食可，睡眠尚可，大小便正常，双下肢无浮肿。否认肝炎、结核等传染病史，否认糖尿病史。体检：体温 37℃，脉搏96次/分，呼吸18次/分，血压136/71mmHg。两肺呼吸音清，心率67次/分，心律齐，无腹壁紧张，无压痛，无肿块。双下肢无浮肿。左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.12mmol/L糖化血红蛋白：5.30%，低密度脂蛋白：2.78mmol/L，甘油三酯：1.47mmol/L，高密度脂蛋白：2.01mmol/L，总胆固醇：4.94mmol/L。Cl：104.89mmol/L，K：3.94mmol/L，Na：141.84mmol/L，P：1.28mmol/L。A/G：1.54，白蛋白：47.17g/L，谷丙转氨酶：8.03u/L，间接胆红素：10.34umol/L，碱性磷酸酶（ALP）：124.50u/L肌酐：46.53umol/L，尿素氮：3.38mmol/L，尿酸：230.30umol/L（20240430松江中心医院），白细胞：-u/L，葡萄糖：-mmol/L，酸碱度：6.5，酮体：-，亚硝酸盐：-，隐血：++。心电图：窦性心律 左室肥大 ST-T改变。B超：脾内实性小结节（脉管瘤可能）双肾囊肿 肝脏、胆囊、胰腺未见明显占位。CT：头颅CT:1.双侧基底节及放射冠区腔隙灶2.老年脑改变。胸部CT：1.双肺多发小结节2.心脏增大，主动脉、冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'612', N'1335', N'患者本人及家属', N'可靠', N'袁纯兰', N'2024-06-05 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'613', N'1336', N'患者本人及家属', N'基本可靠', N'袁纯兰', N'2024-06-07 00:00:00.000', NULL, NULL, N'', N'', N'陈玲娣，女，80岁，记忆力减退五年，加重一月余。患者五年前无明显诱因下出现记忆力减退，发作性不认识自己的家人，外出后不认识回家的路。2024年5月份开始加重，在松江区中心医院就诊，诊断为阿尔茨海默病、认知障碍。予以石杉碱甲片和多奈哌齐平口服。发病以来患者无慢性发热，无胸闷心悸，无呕吐腹痛等，饮食睡眠可。患者因高血压病、冠心病支架植入术后长期口服麝香通心滴丸、硫酸氢氯吡格雷、普伐他汀钠片、盐酸贝尼地平片、尼可地尔片、托拉塞米片等。体检：体温 37℃，脉搏56次/分，呼吸20次/分，血压107/51mmHg。两肺呼吸音粗，心率56次/分，心律齐，腹软，无压痛，四肢无水肿，无肌肉萎缩。生化:葡萄糖：5.12mmol/L。Rbc：3.84×10<sup>12</sup>/L，Hb：123g/L，Wbc：5.39×10<sup>9</sup>/L，中性粒比例：56.5%，淋巴细胞比例：36%，Plt：151×10<sup>9</sup>/L，糖化血红蛋白：5.8%，低密度脂蛋白：2.2mmol/L，甘油三酯：0.99mmol/L，高密度脂蛋白：1.12mmol/L，总胆固醇：3.58mmol/L。Cl：102.29mmol/L，K：4.5mmol/L，Na：142.83mmol/L，白蛋白：44.93g/L，谷丙转氨酶：18.47u/L，碱性磷酸酶（ALP）：69.76u/L，总胆红素：8.66umol/L，球蛋白：24.93g/L。直接胆红素：3.43umol/L。肌酐：97.89umol/L，尿酸：298.36umol/L，心电图：窦性心动过缓，左室肥大，T波改变。B超：肝内钙化灶，双肾钙质沉着，左肾囊肿，胃窦部胃壁增厚，胆囊、胰腺、脾脏未见明显占位。CT：左肺上叶磨玻璃结节，右肺下叶新发实性结节，左肺下叶结节未显示，右肺上下叶钙化灶，左肺下叶纤维灶，心脏增大，主动脉及冠状动脉硬化.两侧放射冠区腔隙灶，老年脑改变（2024.6.4松江区中心医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'616', N'1338', N'患者家属', N'基本可靠', N'胡新志', N'2024-06-20 00:00:00.000', NULL, NULL, N'', N'', N'赵小妹，女，81岁，反复头晕头痛30余年。患者三十年前无诱因出现反复头晕头痛，无恶心呕吐，无肢体活动障碍，在松江中心医院就诊，诊断为“高血压”长期口服氨氯地平片、氯沙坦钾片，血压控制尚稳定。2020年突然出现头晕、右侧肢体活动不利，无意识障碍，在松江中心医院诊断“脑梗死”予以住院治疗（具体治疗不详），病情好转，遗留右侧肢体功能障碍。现因年事已高、个人生活不能自理，于20240620自愿入住我院。入院体检提示：肾功能减退。患者一般情况尚可，轮椅推入病室，反应迟钝、简单对答，有时用手势表达，查体合作，饮食可，睡眠尚可，大小便正常，双下肢轻度凹陷性水肿。有糖尿病史。体检：体温 36.7℃，脉搏78次/分，呼吸18次/分，血压146/67mmHg。两肺呼吸音清，心率78次/分，心律齐，无腹壁紧张，四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为4级,右下肢肌力为5级。生化:葡萄糖：5.6mmol/L，糖化血红蛋白：5.90，低密度脂蛋白：1.79mmol/L，甘油三酯：1.83mmol/L，高密度脂蛋白：1.07mmol/L，总胆固醇：3.46mmol/L。Cl：101.07mmol/L，K：4.23mmol/L，Na：144.17mmol/L，A/G：1.92，白蛋白：45.87g/L，谷丙转氨酶：8.34u/L，间接胆红素：4.40umol/L，碱性磷酸酶（ALP）：103.06u/L，总胆红素：8.15umol/L，总蛋白：69.77g/L。球蛋白：23.90g/L。直接胆红素：3.75umol/L。肌酐：93.36umol/L，尿素氮：5.77mmol/L，尿酸：330.92umol/L，白细胞：51u/L，葡萄糖：-mmol/L，酮体：-，亚硝酸盐：-，心电图：正常心电图。B超：右肾萎缩、右肾结石、右肾囊肿。肝脏、胆囊、胰腺、脾脏、左肾未见明显占位。CT：头颅CT:老年脑改变。胸部CT：1、右肺上叶少许炎症2、双肺小结节3、心脏增大，主动脉及冠状动脉硬化4、甲状腺左叶低密度结节月15mm。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'643', N'1365', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-02-18 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'646', N'1368', N'患者本人及家属', N'基本可信', N'周佳明', N'2025-02-27 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'648', N'1370', N'患者家属', N'可靠', N'涂宝玲', N'2025-03-24 00:00:00.000', NULL, NULL, N'', N'', N'包永珍，女，83岁，记忆力进行性减退伴行为改变4年。家属代诉患者4年前开始出现记忆力进行性减退，丢三落四、易忘事，常常说女儿拿了自己的存折，女儿不管自己了，2023年03月到中心医院神经内科就诊，诊断为“认知障碍”，给予”石杉碱甲片、吡拉西坦“治疗，病情无明显好转，眠差、胡言乱语，2023年09月至2024年02月先后至松江精神卫生中心就诊，诊断为“痴呆、抑郁发作”。予以石杉碱甲片、吡拉西坦、文拉法辛缓释片、疏肝解郁胶囊、右佐匹克隆片等治疗，症状有所改善。发病以来患者无发热，无胸闷气急，无呕吐等，饮食可。目前情绪较为稳定，无明显吵闹、纠缠不清，夜眠尚可未再服用右佐匹克隆片。患者因痴呆、高血压病、糖尿病、脑梗死、骨质疏松症等口服阿卡波糖片、阿托伐他汀钙片、利格列汀片、硫酸氢氯吡格雷片、达格列净片、培哚普利氨氯地平片、碳酸钙D3片(I)、富马酸比索洛尔片、脑心通胶囊等治疗，便秘予以开塞露外用。体检：体温 36.9℃，脉搏67次/分，呼吸20次/分，血压131/64mmHg。两肺呼吸音粗，心率67次/分，心律不齐，腹部软，无压痛，四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫。生化:葡萄糖：6.40mmol/L，Rbc：4.52×10<sup>12</sup>/L，Hb：144.0g/L，Wbc：9.06×10<sup>9</sup>/L，中性粒比例：67.8%，淋巴细胞比例：25.80%，Plt：150.0×10<sup>9</sup>/L，糖化血红蛋白：6.20%，低密度脂蛋白：2.93mmol/L，甘油三酯：1.03mmol/L，高密度脂蛋白：1.98mmol/L，总胆固醇：5.26mmol/L，Ca：2.36mmol/L，Cl：104.0mmol/L，K：4.71mmol/L，Na：142.11mmol/L，P：1.14mmol/L，A/G：1.92，白蛋白：47.55g/L，谷丙转氨酶：15.97u/L，间接胆红素：4.43umol/L，碱性磷酸酶（ALP）：72.88u/L，总胆红素：8.01umol/L，总蛋白：72.29g/L。球蛋白：24.74g/L。直接胆红素：3.58umol/L，肌酐：52.76umol/L，尿素氮：8.29mmol/L，尿酸：242.86umol/L，，白细胞：34.0u/L，尿胆原：阴性（-），葡萄糖：++++，酸碱度：5.5，酮体：阴性（-），上皮细胞：6.0u/L，亚硝酸盐：阴性（-），隐血：阴性（-）。心电图：窦性心律 频发室性早搏 T波改变 V4-V6 低平。 X线胸片：两肺小结节；心脏增大，主动脉及冠状动脉硬化；双肾上腺稍增粗。脾脏包膜钙化灶。B超：胆囊术后未显示，胆总管轻度扩张；肝脏、胰腺、脾脏、双肾未见占位。CT：头颅CT：老年脑改变（20250314上海交通大学学校附属松江医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'652', N'1374', N'患者本人及家属', N'基本可信', N'周佳明', N'2025-04-17 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'654', N'1376', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-04-22 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'656', N'1378', N'患者家属', N'基本可信', N'周佳明', N'2025-04-28 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'657', N'1379', N'患者本人及家属', N'基本可靠', N'周佳明', N'2025-04-30 00:00:00.000', NULL, NULL, N'', N'', N'褚友顺，男，86岁，反复咳嗽咳痰20余年。 患者20余年前开始反复出现咳嗽咳痰，咳白色泡沫痰，无胸闷气促，冬季、季节交换及受凉时咳嗽咳痰尤为明显，予以对症抗炎、止咳化痰等药物治疗（具体治疗不详）症状可缓解。2023年1月出现胸闷气促等不适，曾至当地卫生院就诊，诊断为“慢性阻塞性肺疾病、慢性肺源性肺疾病”，长期服药（茶碱缓释片、托拉塞米片及螺内酯片等）对症治疗，症状可缓解，偶有胸闷气促等不适。患者有高血压病20年余，最高血压（家属诉不详），未口服药物，自诉血压控制可。患者有前列腺增生10年余，长期口服非那雄胺片及盐酸坦索罗辛缓释胶囊对症治疗，自诉症状控制尚可。患者有焦虑状态10年余，长期口服氟哌噻吨美利曲辛片对症治疗，自诉症状控制尚可。因目前动后有气促现象明显，长期吸氧，个人生活不能完全自理于20250430自愿入住我院。发病以来，情绪尚稳定，饮食可，夜间睡眠可，小便可，排便困难予以通便药物治疗可缓解。体检：体温 36.9℃，脉搏85次/分，呼吸18次/分，血压122/73mmHg。两肺呼吸音粗，无干啰音、有哮鸣音。85次/分，心律齐。 无腹壁紧张，无压痛，无反跳痛。四肢无水肿，无肌肉萎缩，肌张力无异常，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：8.67mmol/L，C反应蛋白：22.3mg/LMG/L，糖化血红蛋白：6.1%，肌酐：84.45umol/L，尿素氮：5.2mmol/L，尿酸：527.04umol/L，白细胞：14u/L，管型：0u/L，尿胆原：（—），葡萄糖：（—）mmol/L，酸碱度：5.0，酮体：（—），上皮细胞：0u/L，亚硝酸盐：（—），隐血：（—），心电图：窦性心动过速、室性早搏、频发房性早搏、房性早搏连发、肺型P波，X线胸片：(缺)。B超：慢性胆囊炎、胆囊结石、肝内外胆管扩张、胆总管下段结石、右肾囊肿，CT：1.慢性支气管炎、肺气肿、多发肺大泡形成。2.原右肺中叶小片结影基本吸收。前中纵膈结节与前相仿。3.左肺上叶小钙化灶。4.主动脉弓局部膨隆。5.主动脉及冠状动脉硬化。6.胸7椎体楔形改变。7.胆囊多发结石、胆总管扩张、右肾囊性灶、左肾小结石。8.老年脑改变。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'676', N'1398', N'患者家属', N'基本可信', N'张丁', N'2025-09-12 00:00:00.000', NULL, NULL, NULL, NULL, N'周海妹，女，91岁，反复头晕8年余患者8年余前无明显诱因反复出现头晕不适，无明显头痛，无恶心、呕吐，无黒朦，无视物旋转，无胸闷心悸，曾就诊于上海市松江区方塔中医医院，最高血压200/110mmHg，诊断“高血压病”，予对症治疗（具体不详）。患者近期未服药，血压维持在120-150/70-90mmHg之间。发病以来，患者无头晕头痛，无发热，无呕吐，无明显消瘦等，饮食睡眠可。患者因睡眠障碍、慢性胃炎目前服用阿普唑仑片、归脾合剂、奥美拉唑肠溶胶囊、香砂养胃颗粒。有腹股沟疝、冠状动脉粥样硬化性心脏病、脑动脉供血不足、血吸虫病、右肺占位等病史。体检：体温 37.0℃，脉搏82次/分，呼吸20次/分，血压148/82mmHg。两肺呼吸音粗，心率80次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：7.37mmol/L。糖化血红蛋白：5.7%，低密度脂蛋白：5.10mmol/L，甘油三酯：4.48mmol/L，高密度脂蛋白：1.09mmol/L，总胆固醇：7.01mmol/L。Ca：2.31mmol/L，Cl：104.24mmol/L，K：3.78mmol/L，Na：143.17mmol/L，P：0.96mmol/L。A/G：1.51，白蛋白：38.53g/L，谷丙转氨酶：12.82u/L，间接胆红素：5.25umol/L，碱性磷酸酶（ALP）：97.14u/L，总胆红素：9.98umol/L，总蛋白：64.09g/L。球蛋白：25.56g/L。直接胆红素：4.73umol/L。肌酐：53.31umol/L，尿素氮：3.61mmol/L，尿酸：160.74umol/L。心电图：窦性心律 ST-T改变。B超：血吸虫病肝硬化。胰腺、脾脏、双肾未见明显占位。胆囊术后未显示，肝内外胆管未见明显扩张。胸部CT：1.右肺下叶占位，较前增大，建议增强检查。2.双肺多发结节。3.心脏增大，主动脉及部分冠状动脉硬化。4.附见：甲状腺右叶增大，密度不均。血吸虫肝硬化征象。胆囊未见显示。颅脑CT：老年脑改变。（上海交通大学医学院附属松江医院20250909）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'573', N'1296', N'患者本人及家属', N'基本可信', N'庄秋丽', N'2023-09-14 00:00:00.000', NULL, NULL, NULL, NULL, N'许阿妹，女，93岁，患者两下肢无力行走欠稳欠利5年。患者5年前不慎跌倒，引起腰椎骨折，于上海第六人民医院手术予以治疗，目前腰背部仍反复酸痛不适，两下肢无力，行走欠稳欠利。
患者原有贫血史20余年，反复出现头晕不适，曾多次入院查找贫血原因不明，目前检查结果为重度贫血。
患者原有胃溃疡史10年左右，反复出现腹胀腹痛不适，平时常服用雷贝拉唑钠肠溶片护胃。
患者便秘5-6年，平时大便干结难解，长期服用龙荟丸通便。
患者本次入院前体检（中心医院）CT：双侧基底节区腔隙灶，老年脑改变。慢性支气管炎，两肺散在少许纤维灶，左肺下叶及右肺上叶钙化灶，主动脉及冠状动脉硬化，贫血征象，两侧少量胸腔积液。B超：肝脏囊肿，慢性胆囊炎、胆囊结石、胆总管扩张，左肾囊肿、左肾轻度积水。心电图：窦性心律，频发房性早搏，ST改变，V4-V6水平压低。
患者目前一般情况可，无发热，纳食可，小便可，大便用通便药能解，夜间睡眠安。
体检：体温 36.8℃，脉搏93次/分，呼吸20次/分，血压108/58mmHg。神志清，精神可，贫血貌，消瘦，呼吸平稳，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音粗，未及明显干湿啰音，心率93次/分，律不齐，闻及5－10次/分早搏，腹软，全腹无压痛，肝脾肋下未及肿大，两下肢膝关节以下水肿，两下肢肌力为4级。
生化:葡萄糖：5.54mmol/L。Rbc：1.47×10<sup>12</sup>/L，Hb：58g/L，Wbc：2.33×10<sup>9</sup>/L，中性粒比例：45.90%，淋巴细胞比例：41.20%，Plt：341×10<sup>9</sup>/L。C反应蛋白：0.75MG/L，糖化血红蛋白：5.00%，低密度脂蛋白：1.23mmol/L，甘油三酯：0.53mmol/L，高密度脂蛋白：1.37mmol/L，总胆固醇：2.61mmol/L。Ca：2.09mmol/L，Cl：101.24mmol/L，K：4.05mmol/L，Na：138.16mmol/L，P：1.10mmol/L。A/G：1.48，白蛋白：37.89g/L，谷丙转氨酶：4.97u/L，间接胆红素：8.00umol/L，碱性磷酸酶（ALP）：48.84u/L，总胆红素：16.46umol/L，总蛋白：63.43g/L。球蛋白：25.54g/L。直接胆红素：8.46umol/L。肌酐：45.33umol/L，尿素氮：5.23mmol/L，尿酸：269.60umol/L。心电图：窦性心律，频发房性早搏，ST改变，V4-V6水平压低。B超：肝脏囊肿，慢性胆囊炎、胆囊结石、胆总管扩张，左肾囊肿、左肾轻度积水。CT：双侧基底节区腔隙灶，老年脑改变。慢性支气管炎，两肺散在少许纤维灶，左肺下叶及右肺上叶钙化灶，主动脉及冠状动脉硬化，贫血征象，两侧少量胸腔积液。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'585', N'1308', N'患者家属', N'基本可信', N'庄秋丽', N'2024-01-24 00:00:00.000', NULL, NULL, NULL, NULL, N'陈文峰，男，77岁，自知力认知能力差8年左右。患者自2016年起出现行为异常，强迫重复做同一件事，当时未就诊做相关检查。大约2018年起患者渐进性出现自知力认知能力下降，常自言自语，记忆力下降，当时于松江中心医院就诊做CT检查提示“脑萎缩”。
患者原有原发性高血压病30年左右，平时未主诉有头晕头痛等不适，常服用苯磺酸氨氯地平片降压，据说血压控制可。
患者目前一般情况尚可，无发热，纳食可，因前列腺增生排尿困难目前留置导尿，导尿管畅，大便无异常。患者自2023年12月25日生病住院至今无力下床行走，目前卧床。体检：体温 36.6℃，脉搏81次/分，呼吸20次/分，血压142/80mmHg。神清，巩膜清，唇不绀，两肺呼吸音清，心率81次/分，心律齐，腹软，全腹无压痛，四肢无水肿。生化2023.12.28中心医院:葡萄糖：5.36mmol/L。Rbc：3.7×10<sup>12</sup>/L，Hb：113g/L，Wbc：8.9×10<sup>9</sup>/L，中性粒比例：65.1%，Plt：228×10<sup>9</sup>/L。C反应蛋白：12.08MG/L，低密度脂蛋白：1.83mmol/L，甘油三酯：1.16mmol/L，高密度脂蛋白：1.11mmol/L，总胆固醇：3.52mmol/L。Ca：2.08mmol/L，Cl：114.36mmol/L，K：3.59mmol/L，Na：149.02mmol/L，P：0.88mmol/L。A/G：1.33，白蛋白：32.07g/L，谷丙转氨酶：31.34u/L，间接胆红素：4.28umol/L，碱性磷酸酶（ALP）：80.89u/L，总胆红素：7.8umol/L，总蛋白：56.23g/L。球蛋白：24.16g/L。直接胆红素：3.52umol/L。肌酐：84.56umol/L，尿素氮：7.33mmol/L，尿酸：334.46umol/L，白细胞：11/uLu/L，管型：0u/L，尿胆原：阴性（-），葡萄糖：+++mmol/L，酸碱度：6.5，酮体：阴性（-），上皮细胞：0u/L，亚硝酸盐：阴性（-），隐血：+++。CT：（2023.12.25中心医院）头颅CT：右侧基底节区腔隙灶，脑白质变性，老年脑改变。腹部CT：结肠散在积气积粪，双肾及输尿管上段积水扩张，脂肪肝，肝右叶囊性灶，胆囊密度混杂，结石可能，食管裂孔疝。胸部CT：右肺上叶小斑片炎症改变。双肺下叶纤维灶，心包少量积液，心脏增大，主动脉、冠状动脉硬化，双侧少量胸腔积液。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'590', N'1313', N'患者家属', N'基本可靠', N'胡新志', N'2024-02-27 00:00:00.000', NULL, NULL, N'', N'', N'张顺娟，女，88岁，双下肢无力行动不便半年。患者长期居住在松江中山街道敬老院，半年前无诱因出现双下肢无力，站立不稳，行走不便，无头晕头痛，无恶心呕吐，无视物旋转，无黒朦，未到医院进一步诊治，无药物治疗，长期卧床，个人生活不能自理。于20240227自愿转入我院。患者一般情况尚可，轮椅推入病室，对答切题，查体合作，饮食可，睡眠尚可，大小便正常。
入院体检提示：腔隙性脑梗、慢性支气管炎、胆总管结石、高胆固醇血症。否认肝炎、结核等传染病史。否认高血压、糖尿病史体检：体温 36.6℃，脉搏73次/分，呼吸18次/分，血压132/78mmHg。两肺呼吸音清，73次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无肿块。四肢无水肿，左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。右手腕部骨折史，尾骨骨折史生化:葡萄糖：7.21mmol/L。低密度脂蛋白：3.76mmol/L，甘油三酯：0.92mmol/L，高密度脂蛋白：1.33mmol/L，总胆固醇：5.29mmol/L。Ca：1.52mmol/L，Cl：101.87mmol/L，K：3.57mmol/L，Na：142.28mmol/L，A/G：0.91，白蛋白：35.61g/L，白细胞：187u/L，尿胆原：+，葡萄糖：-mmol/L，酸碱度：5.5，酮体：-，亚硝酸盐：++，隐血：+。心电图：交界性心动过速 肢体导联低电压 T波改变。B超：胆囊炎、胆囊肿大、肝内外胆管扩张、胆总管内结石，肝脏胰腺脾脏、双肾未见明显占位。CT：头颅CT:1.量基底节腔隙灶2.老年脑改变。胸部CT:1.心脏增大，主动脉、冠状动脉硬化，肺淤血2.慢性支气管炎改变，右肺上叶纤维、钙化灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'631', N'1353', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-09-05 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'637', N'1359', N'患者家属', N'基本可信', N'周佳明', N'2024-10-16 00:00:00.000', NULL, NULL, NULL, NULL, N'吴全英宝，女，73岁，左侧肢体乏力12年余。患者家属代诉患者12年前无明显诱因下突发左侧肢体活动不利，由家属送至松江区中心医院就诊，诊断：脑梗死，予对症治疗后病情基本稳定。近年来患者生活自理能力逐步减退，遗留有左侧肢体乏力，左上肢肌肉挛缩。1年前因车祸出现左侧肢体乏力症状加重，经对症治疗后病情基本稳定，遗留有吞咽功能减退、口齿欠清，并伴有记忆减退，语言功能轻度受损，自知力欠佳。曾至松江区精神卫生中心就诊，诊断：血管性痴呆，未予以口服药物治疗。患者有高血压病10年余，目前未口服药物治疗，血压控制不详。有糖尿病8年余，目前口服艾托格列净片及阿卡波糖片降糖治疗，自诉血糖控制尚可。有心房颤动6年余，长期口服甲磺酸艾多沙班片抗凝及琥珀酸美托洛尔缓释片控制心室率治疗，目前病情较为稳定。现因个人生活不能自理，于20241016自愿入院。入院体检提示：两肺多发结节，两肺下叶少量支扩伴炎症。患者一般情况尚可，扶入病房，简单对答，口齿欠清，查体合作，饮食可，睡眠可，二便可。体检：体温 36.2℃，脉搏98次/分，呼吸19次/分，血压117/77mmHg。两肺呼吸音粗，100次/分，心律不齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，有肌肉萎缩，肌张力增强，无偏瘫,左上肢肌力为4级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.8mmol/L。Rbc：4.69×10<sup>12</sup>/L，Hb：136g/L，Wbc：7.22×10<sup>9</sup>/L，中性粒比例：63.6%，淋巴细胞比例：29.2%，Plt：155×10<sup>9</sup>/L。糖化血红蛋白：6.5%，低密度脂蛋白：2.11mmol/L，甘油三酯：2.01mmol/L，高密度脂蛋白：1.01mmol/L，总胆固醇：3.79mmol/L。Ca：2.31mmol/L，Cl：108.53mmol/L，K：4.05mmol/L，Na：145.56mmol/L，P：1.34mmol/L。A/G：1.78，白蛋白：42.6g/L，直接胆红素：3.44umol/L。肌酐：35.09umol/L，尿素氮：4.64mmol/L，尿酸：185.39umol/L，心电图：心房颤动。X线胸片：右股骨大粗隆骨折。B超：胆囊壁稍毛糙，右侧肾钙化灶可能。CT：1.两肺多发结节2.两肺下叶少量支扩伴炎症，两肺少量纤维灶4.心脏稍大，主动脉、冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'645', N'1367', N'患者本人及家属', N'可信', N'涂宝玲', N'2025-02-27 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'659', N'1381', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-05-06 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'662', N'1384', N'患者家属', N'基本可信', N'涂宝玲', N'2025-05-13 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'664', N'1386', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-05-22 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'666', N'1388', N'患者本人及家属', N'可靠', N'袁纯兰', N'2025-05-28 00:00:00.000', NULL, NULL, N'', N'', N'陈巧林，男，64岁，右肺癌术后4年余。患者于2021年10月因肺癌在上海市第一人民医院手术治疗，2021年11月发现脑转移再次予以手术治疗，术后恢复尚可。2022年1月癫痫发作，予以丙戊酸钠缓释片、左乙拉西坦片等口服后发作次数减少，近3年来未有大发作，因生活不能完全自理今入住我福利院。发病以来患者无高热，无胸闷胸痛，无呕吐腹泻等，饮食可，睡眠障碍予以艾司唑仑助眠。患者既往高血压病史，目前口服苯磺酸左氨氯地平片，血压控制满意；因甲状腺功能减退予以左甲状腺素钠片治疗。既往高血压病6年、右侧肺癌术后3年，脑转移癌术后3年，甲状腺功能减低1年，窦性心动过缓5年，睡眠障碍1年，胃癌穿孔修补术后30年，肋骨、腰椎横突骨折术后9月。体检：体温 36.3℃，脉搏59次/分，呼吸18次/分，血压119/74mmHg。两肺呼吸音清心率59次/分，心律齐，，腹软无压痛，生化:葡萄糖：5.28mmol/L，，低密度脂蛋白：3.92mmol/L，甘油三酯：1.31mmol/L，高密度脂蛋白：0.9mmol/L，总胆固醇：5.04mmol/L，Cl：106.3mmol/L，K：3.98mmol/L，Na：140.8mmol/L，白蛋白：38.2g/L，谷丙转氨酶：15u/L，间接胆红素：5.9umol/L，碱性磷酸酶（ALP）：51u/L，总胆红素：8.8umol/L，总蛋白：69.3g/L。球蛋白：31.1g/L。直接胆红素：2.9umol/L。肌酐：64.1umol/L，尿素氮：6mmol/L，尿酸：381umol/L心电图：窦性心动过缓（2025.5.22上海市第一人民医院）。B超：胰腺体部低回声区，胆囊壁胆固醇结晶，胆囊壁欠光滑，左肾囊肿，前列腺增生；CT：左额叶占位术后改变，左侧额叶软化灶可能，脑内数枚环形高密度结节，转移灶？脑萎缩，脑白质病，蝶窦炎症；左肺术后改变，右侧胸膜增厚及少量胸腔积液，右肺门影增大，左肺上叶间隔旁肺气肿，左肺上叶舌段及下叶部分不张、实变，主动脉及冠脉硬化，PORT置入中（2025.5.22上海市第一人民医院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'667', N'1389', N'患者本人及家属', N'基本可信', N'周佳明', N'2025-06-10 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'670', N'1390', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-06-19 00:00:00.000', NULL, NULL, NULL, NULL, N'宋民力，男，60岁，智力低下60年。  患者自幼智力低下，生活不能完全自理，能听懂简单的语言，能自行吃饭、穿脱衣及洗澡。因年事渐高，于2025年6月19日由上海市第五社会福利院转入本院。发病以来一般情况可，纳可，大小便正常，夜眠可。陈旧性肺结核，房性早搏，完全性右束支合并左前支阻滞，骨质疏松，支气管扩张，乙型病毒性肝炎体检：体温 36.6℃，脉搏73次/分，呼吸20次/分，血压134/80mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。73次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.2mmol/L。Rbc：4.56×10<sup>12</sup>/L，Hb：136g/L，Wbc：8.5×10<sup>9</sup>/L，中性粒比例：80.7%，淋巴细胞比例：13.9%，Plt：142×10<sup>9</sup>/L。C反应蛋白：2.31MG/L，糖化血红蛋白：5.8%，低密度脂蛋白：2.35mmol/L，甘油三酯：1.2mmol/L，高密度脂蛋白：1.19mmol/L，总胆固醇：3.84mmol/L。Ca：2.29mmol/L，Cl：108mmol/L，K：4.0mmol/L，Na：144mmol/L，P：0.77mmol/L。A/G：1.57，白蛋白：44g/L，谷丙转氨酶：9u/L，间接胆红素：1umol/L，碱性磷酸酶（ALP）：72u/L，总胆红素：9umol/L，总蛋白：72g/L。球蛋白：28g/L。直接胆红素：8umol/L。肌酐：93umol/L，尿素氮：5.1mmol/L，尿酸：0.37umol/L，白细胞：1u/L，管型：阴性u/L，尿胆原：正常，葡萄糖：阴性mmol/L，酸碱度：5.5，酮体：阴性，上皮细胞：0u/L，亚硝酸盐：阴性，隐血：弱阳性。心电图：1、窦性心律2、房性早搏3、完全性右束支合并左前分支阻滞X线胸片：无B超：双肾实质回声稍增强CT：1、两侧放射冠区腔隙灶，请结合临床2、两肺散在慢性感染及索条灶；右上肺钙化灶。请结合临床。随访3、右侧部分肋骨扭曲')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'677', N'1397', N'患者本人及家属', N'可靠', N'包渊', N'2025-09-11 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'632', N'1354', N'患者家属', N'基本可靠', N'胡新志', N'2024-09-10 00:00:00.000', NULL, NULL, N'', N'', N'杨美华，女，76岁，反复头晕、头痛10余年。体温 36.4℃，脉搏74次/分，呼吸16次/分，血压116/78mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。74次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为4级。生化:葡萄糖：5.76mmol/L。Rbc：5.44×10<sup>12</sup>/L，Hb：152.00g/L，Wbc：6.07×10<sup>9</sup>/L，中性粒比例：63.00%，淋巴细胞比例：23.10%，Plt：181.00×10<sup>9</sup>/L。C反应蛋白：-MG/L，糖化血红蛋白：6.30%，低密度脂蛋白：3.01mmol/L，甘油三酯：1.16mmol/L，高密度脂蛋白：1.13mmol/L，总胆固醇：4.47mmol/L。Ca：2.31mmol/L，Cl：103.03mmol/L，K：3.81mmol/L，Na：140.45mmol/L，P：1.05mmol/L。A/G：1.30，白蛋白：42.83g/L，谷丙转氨酶：6.91u/L，间接胆红素：4.60umol/L，碱性磷酸酶（ALP）：74.72u/L，总胆红素：7.71umol/L，总蛋白：75.73g/L。球蛋白：32.90g/L。直接胆红素：3.11umol/L。肌酐：58.46umol/L，尿素氮：5.71mmol/L，尿酸：255.25umol/L，心电图：窦性心动过缓 I°房室传导阻滞B超：1.脂肪肝、肝脏囊肿；2.双肾肾盂扩张、右肾囊肿；3.胆囊、胰腺、脾脏未见明显占位CT：胸部CT：双乳腺术后缺如，右侧前上胸壁软组织肿胀、部分肌肉结构显示不清。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'658', N'1380', N'患者家属', N'基本可靠', N'张丁', N'2025-04-30 00:00:00.000', NULL, NULL, N'', N'', N'翁应英，男，77岁，时有头晕18年余。体检：体温 36.7℃，脉搏73次/分，呼吸19次/分，血压143/78mmHg。两肺呼吸音粗，心率73次/分，心律齐，无腹壁紧张，无压痛，无反跳痛。左上肢肌力为5级,左下肢肌力为3级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：（上海市第一人民医院20250425）11.56mmol/L。Rbc：4.89×10<sup>12</sup>/L，Hb：152g/L，Wbc：7.53×10<sup>9</sup>/L，中性粒比例：77.6%，淋巴细胞比例：17.2%，Plt：160×10<sup>9</sup>/L（上海市第一人民医院20250425）。糖化血红蛋白：7.5%，低密度脂蛋白：2.80mmol/L，甘油三酯：1.17mmol/L，高密度脂蛋白：1.12mmol/L，总胆固醇：4.44mmol/L。Cl：103.6mmol/L，K：3.67mmol/L，Na：140.9mmol/L，A/G：1.9，白蛋白：41.8g/L，谷丙转氨酶：6u/L，间接胆红素：12.9umol/L，碱性磷酸酶（ALP）：84u/L，总胆红素：18.7umol/L，总蛋白：63.6g/L。球蛋白：21.8g/L。直接胆红素：5.8umol/L。肌酐：59.8umol/L，尿素氮：12.4mmol/L，尿酸：341umol/L，尿胆原：-，葡萄糖：++++mmol/L，酸碱度：5.0，酮体：-，亚硝酸盐：-，隐血：-。心电图：（上海市第一人民医院20250425）窦性心动过缓。B超：（上海市第一人民医院20250425）肝脏、胆囊、胰腺、脾脏未见明显异常。CT：（上海市第一人民医院20250425）胸部CT：慢支肺气肿改变，双肺间质性增生。主动脉硬化。颅脑CT：右侧半卵圆中心高密度影，血肿？建议结合病史进一步检查。双侧基底节区腔隙灶。脑萎缩，脑白质病。脑内动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'626', N'1348', N'患者家属', N'基本可靠', N'胡新志', N'2024-07-24 00:00:00.000', NULL, NULL, N'', N'', N'潘雪妹，女，97岁，反复咳嗽咳痰5年，加重1周。患者既往体健，5年前无明显诱因下出现咳嗽咳痰，冬春季较明显，长期慢性咳嗽咳痰，无胸闷气促，未曾至当地医院就诊，自行服用抗生素、止咳化痰等药物（具体不详）。1周前咳嗽咳痰加重，痰呈白粘痰，不易咳出，无胸闷气促，无咯血及胸痛等现象。现因年事已高、个人生活不能自理，于20240724自愿入住我院。入院体检提示：有贫血、低蛋白血症、慢性支气管炎、肝囊肿、胆囊结石、心律失常、频发房性早搏史。患者一般情况尚可，轮椅推入病室，反应迟钝，查体合作，饮食可，睡眠尚可，大小便正常，双下肢无水肿。否认肝炎、结核等传染病史。有眩晕症病史14年。有双膝关节炎病史8年。体检：体温 36.3℃，脉搏97次/分，呼吸18次/分，血压86/45mmHg。两肺呼吸音清，心率97次/分，心律不齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，左上肢肌力为4级,左下肢肌力为4级,右上肢肌力为4级,右下肢肌力为4级。生化:葡萄糖：5.68mmol/L。Rbc：Hb：91g/L，，糖化血红蛋白：5.4%低密度脂蛋白：2.28mmol/L，甘油三酯：1.07mmol/L，高密度脂蛋白：0.90mmol/L，总胆固醇：3.38mmol/L。Ca：2.21mmol/L，Cl：102.3mmol/L，K：4.49mmol/L，Na：136.2mmol/L，P：1.03mmol/L。A/G：0.66，白蛋白：29.60g/L，谷丙转氨酶：9.30u/L，总蛋白：74.67g/L。球蛋白：45.07g/L。肌酐：70.1umol/L，尿素氮：4.46mmol/L，尿酸：287.2umol/L白细胞：2+u/L。亚硝酸盐：+，心电图：1.窦性心律不齐 2.频发性房性早搏 3.T波变化（II、III、avF、v3-v6低平B超：1.肝内液性占位-囊肿可能 2.胆囊多发胆固醇结晶 3.左肾盂轻度扩张 4.胰腺、脾脏、右肾未见明显异常。CT：1.老年脑改变 2.两肺散在炎症 3.心脏增大，主动脉及左冠状动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'627', N'1349', N'患者家属', N'可信', N'王屹', N'2024-08-01 00:00:00.000', NULL, NULL, NULL, NULL, N'王鸣权，男，71岁，左侧肢体活动不利3年。患者3年前无明显诱因下突发左侧肢体活动不利，伴言语不清，经松江区中心医院诊治，诊断为：脑梗死，予阿司匹林肠溶片抗血小板凝聚、银杏叶活血化瘀通络等对症治疗，症状基本稳定。患者发病后左上肢活动障碍，生活自理能力明显下降，今由家属送入本院住养。患者原有高血压史20年，予硝苯地平缓释片治疗；糖尿病史8年，予西格列汀二甲双胍片治疗；前列腺增生2年，予那雄胺片、坦索罗辛胶囊治疗。患者本次入院体检提示：胆囊炎、胆结石、脂肪肝、冠状动脉粥样硬化性心脏病。患者目前精神状态可，口齿不清，纳可，睡眠可，大小便较正常。脑出血史20年。体检：体温 36.7℃，脉搏72次/分，呼吸18次/分，血压136/71mmHg。两肺呼吸音清，心率72次/分，心律齐，腹软，无压痛，无反跳痛，双下肢浮肿，无肌肉萎缩，肌张力无异常，左上肢肌力为3级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：6.13
mmol/L（松江医院2024.7.26）。Rbc：4.71×10<sup>12</sup>/L，Hb：141g/L，Wbc：7.86×10<sup>9</sup>/L，中性粒比例：56%，淋巴细胞比例：36.3%，Plt：250
×10<sup>9</sup>/L（松江医院2024.7.7）。糖化血红蛋白：6.10
%（松江医院2024.7.26），低密度脂蛋白：2.26
mmol/L（松江医院2024.7.26），甘油三酯：1.24mmol/L，高密度脂蛋白：0.70mmol/L，总胆固醇：3.47mmol/L。（松江医院2024.7.26）。A/G：1.21，白蛋白：41.12g/L，谷丙转氨酶：19.48u/L，间接胆红素：3.39umol/L，碱性磷酸酶（ALP）：109.30
u/L（松江医院2024.7.26），总胆红素：6.33umol/L，总蛋白：75.04g/L。球蛋白：33.92g/L。直接胆红素：2.94umol/L。肌酐：95.46umol/L，尿素氮：5.23mmol/L，尿酸：388.45
umol/L（松江医院2024.7.26），心电图：正常心电图
（松江医院2024.7.7）B超：脂肪肝、胆囊炎、胆囊结石
（松江医院2024.7.27）CT：右外囊区软化灶、双侧放射冠区腔隙灶、老年脑改变、两肺上叶结节、主动脉及冠状动脉硬化
（松江医院2024.7.7）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'629', N'1351', N'患者家属', N'基本可靠', N'胡新志', N'2024-08-13 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'630', N'1352', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-08-14 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'640', N'1362', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2024-10-29 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'636', N'1358', N'患者家属', N'基本可信', N'庄秋丽', N'2024-10-15 00:00:00.000', NULL, NULL, NULL, NULL, N'孙金芳，女，78岁，认知能力障碍10余年。患者进行性认知能力下降10年左右，开始时反应迟钝，言语减少，4年前开始症状逐渐加速严重，目前自知力认知能力差，不能正常交流。曾于松江中心医院就诊，头颅CT：脑萎缩。诊断：阿尔茨海默病，目前予以抗精神疾病药物干预。
患者原有原发性高血压病30余年，最高血压（家属诉不详），长期服药，据说血压控制可。
患者原有2型糖尿病史10余年，长期服药，据说血糖控制尚可，无多饮多食多尿等现象。
患者原有帕金森症4年左右，开始表现为两下肢行走僵硬，踮着脚尖行走，跨步难，2年前开始四肢僵硬，两下肢不能移步，不能独自站立，需人扶助尚可站立。
目前一般情况尚可，无发热，纳食可，两便无异常。
体检：体温 37.2℃，脉搏93次/分，呼吸20次/分，血压129/75mmHg。神志清，呼吸平稳，T37.2°C，BP129/75mmHg，氧饱和度95%，巩膜清，唇不绀，颈软，颈静脉无怒张，两肺呼吸音清，心率93次/分，律齐，腹软，全腹无压痛，四肢无水肿，肌张力增强，两膝关节僵硬屈曲，不能伸直，左上肢肌力为4级，左下肢肌力为3级，右上肢肌力为4级，右下肢肌力为3级。生化:葡萄糖：7.72mmol/L。Rbc：4.08×10<sup>12</sup>/L，Hb：134g/L，Wbc：5.71×10<sup>9</sup>/L，中性粒比例：56.20%，淋巴细胞比例：35%，Plt：174×10<sup>9</sup>/L。糖化血红蛋白：6.90%，低密度脂蛋白：2.49mmol/L，甘油三酯：0.98mmol/L，高密度脂蛋白：1.78mmol/L，总胆固醇：4.51mmol/L。Ca：2.25mmol/L，Cl：107.39mmol/L，K：4.40mmol/L，Na：143.60mmol/L，P：1.11mmol/L。A/G：1.70，白蛋白：40.28g/L，谷丙转氨酶：11.41u/L，间接胆红素：7.71umol/L，碱性磷酸酶（ALP）：118.66u/L，总胆红素：12.34umol/L，总蛋白：63.91g/L。球蛋白：23.63g/L。直接胆红素：4.63umol/L。肌酐：64.82umol/L，尿素氮：4.99mmol/L，尿酸：271.44umol/L，心电图：窦性心律，完全性右束支传导阻滞，异常Q波（III、AVF）。B超：肝内钙化灶，慢性胆囊炎、胆囊胆固醇结晶，双肾弥漫性病变、右肾囊肿、右肾微小结石，胰腺显示不清，脾脏未见明显占位。CT：老年脑改变，右侧第4腋肋扭曲，胸5、8椎体变扁，两肺纤维灶，主动脉硬化。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'639', N'1361', N'患者家属', N'基本可信', N'周佳明', N'2024-10-24 00:00:00.000', NULL, NULL, NULL, NULL, N'周三秀，女，81岁，认知能力障碍15年余。患者进行性认知能力下降15年左右，开始时反应迟钝，言语减少，时有胡言乱语等症状，半年前因摔倒后症状加重，曾至上海同德医院就诊，诊断：硬膜下血肿、侧脑室出血、混合性痴呆、右侧股骨粗隆间骨折，予以保守及康复对症治疗后，目前自知力认知能力差，只能简单对答交流，右侧下肢功能障碍。患者原有原发性高血压病2余年，最高血压（家属诉不详），目前未口服药物治疗，自诉血压控制可。患者原有脑梗死1余年，目前未口服药物治疗，遗留有左下侧肢体乏力等症状。患者原有睡眠障碍半年左右，目前未口服药物治疗，自诉夜眠尚可。目前因年事已高、个人生活不能自理于20241024自愿入住我院。患者一般情况尚可，推入病房，简单对答，饮食可、睡眠可，大小便正常。否认肝炎、结核等传染病史，否认有冠心病、糖尿病等慢性病史体检：体温 36.6℃，脉搏70次/分，呼吸18次/分，血压116/62mmHg。两肺呼吸音粗，70次/分，心律齐。无腹壁紧张，无压痛，无反跳痛。 四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为4级,右上肢肌力为5级,右下肢肌力为3级。生化:葡萄糖：4.92 mmol/L（20240320中山医院）。Rbc：3.41×10<sup>12</sup>/L，Hb：106g/L，Wbc：6.09×10<sup>9</sup>/L，中性粒比例：65%，淋巴细胞比例：23.1%，Plt：61×10<sup>9</sup>/L（20241018同德医院）。C反应蛋白：1.73MG/L，糖化血红蛋白：5.4%（20240320中山医院），低密度脂蛋白：3.1mmol/L，甘油三酯：0.9mmol/L，高密度脂蛋白：1.21mmol/L，总胆固醇：4.37mmol/L。Ca：1.27mmol/L，Cl：101.35mmol/L，K：3.26mmol/L，Na：139.75mmol/L，A/G：1.43，白蛋白：35.5g/L，碱性磷酸酶（ALP）：107.1u/L，总胆红素：5.85umol/L，总蛋白：60.4g/L。球蛋白：24.8g/L。直接胆红素：2.04umol/L。肌酐：44.5umol/L，尿素氮：6.12mmol/L，尿酸：274.8umol/L，心电图：1.窦性心律，2.T波改变，3.顺时针转位。（20241018同德医院）X线胸片：1.慢性支气管炎，2.主动脉型心脏，主动脉增宽迂曲。（20241018同德医院）B超：胆囊切除术后。（20240320中山医院）头颅CT:1.右侧枕顶部慢性硬膜下血肿，2.左侧枕部术后，3.两侧基底节区多发腔隙性梗塞灶。胸部CT:1.左肺上叶微小结节，两肺散在慢性炎症，2.食管下段壁增厚。(20240320中山医院)')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'651', N'1373', N'患者家属', N'可靠', N'涂宝玲', N'2025-03-27 00:00:00.000', NULL, NULL, N'', N'', NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'653', N'1375', N'患者家属', N'基本可信', N'涂宝玲', N'2025-04-18 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'655', N'1377', N'患者家属', N'可靠', N'袁纯兰', N'2025-04-23 00:00:00.000', NULL, NULL, N'', N'', N'沈引弟，女，97岁，时有头晕头痛15年余。患者15余年前无诱因下出现头晕头痛，无恶心呕吐，无意识障碍，于上海市松江区中心医院就诊，诊断“高血压病”，予以缬沙坦胶囊降压治疗症状有所缓解，定期社区卫生服务中心配药，目前血压稳定。发病以来，患者无发热，无呕吐，无明显消瘦等，饮食睡眠可。 
   患者因冠心病、睡眠障碍、脑梗塞后遗症目前长期口服托拉塞米片、艾司唑仑片、培元通脑胶囊等治疗。冠心病史5年，脑供血不足5年，脑梗死3年，慢性支气管炎3年，高尿酸血症3年，曾服用非布司他、百令胶囊等，胃溃疡3年，睡眠障碍1年，趾骨骨折近2年，慢性胆囊炎3年。体检：体温 36.4℃，脉搏74次/分，呼吸18次/分，血压109/75mmHg。两肺呼吸音清，无干啰音、无哮鸣音、无 湿啰音、无胸膜摩擦音、无异常呼吸音、语音传导 无异常。74次/分，心律齐，未闻及早搏,第一心音无增强或减弱，各瓣膜区 未闻及杂音，无心包摩擦音。无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。 无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：5.25mmol/L。Rbc：4.14×10<sup>12</sup>/L，Hb：124g/L，Wbc：6.29×10<sup>9</sup>/L，中性粒比例：54.4%，淋巴细胞比例：35.6%，Plt：260×10<sup>9</sup>/L（2025.4.8松江区中心医院）。糖化血红蛋白：5.6%，低密度脂蛋白：3.94mmol/L，甘油三酯：1.55mmol/L，高密度脂蛋白：1.53mmol/L，总胆固醇：5.67mmol/L，Cl：105.27mmol/L，K：4.74mmol/L，Na：142.37mmol/L，A/G：1.38，白蛋白：40.4g/L，谷丙转氨酶：12.81u/L，间接胆红素：4.8umol/L，碱性磷酸酶（ALP）：91.93u/L，总胆红素：9.05umol/L，总蛋白：69.6g/L。球蛋白：29.2g/L。直接胆红素：4.25umol/L。肌酐：82.58umol/L，尿素氮：7.51mmol/L，尿酸：370.12umol/L，白细胞：8u/L，尿胆原：+，葡萄糖：-mmol/L，隐血：-。心电图：窦性心律，房性早搏X线胸片：(缺)。B超：胆囊肿大，慢性胆囊炎可能，胆囊结石，胆囊内实性占位，胆总管轻度扩张，胰腺颈部囊性结节，脾脏偏小，双肾弥漫性病变，双肾囊肿，双肾结石CT：老年脑改变，右肺上叶新发结节，左肺上叶小结节，右肺上叶小片结影，两肺多个肺气囊，心脏增大，主动脉硬化，胆囊结石，双肾囊性灶（2025.4.8松江区中心医院）。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'668', N'1392', N'其他', N'可靠', N'袁纯兰', N'2025-06-19 00:00:00.000', NULL, NULL, N'第五福利院', N'', N'罗小义，男，60岁，智力低下60年。患者自幼智力低下，生活不能完全自理，能听懂简单的语言，能自行吃饭、穿脱衣及洗澡。因年事渐高，于2025年6月19日由上海市第五社会福利院转入本院。本次入院体检提示窦性心动过缓，两侧放射冠区腔隙灶，乙型病毒性肝炎，左肝实质性结节，胆囊息肉，右肾囊肿，左肾结石，发病以来患者一般情况可，纳可，大小便正常，夜眠可。
患者既往高血压病史，目前口服替米沙坦氢氯噻嗪片，血压控制可。患者既往高血压病数年，窦性心动过缓，胆囊息肉，右肾囊肿，左肾结石，骨量减少，乙型病毒性肝炎。体检：体温 36.1℃，脉搏64次/分，呼吸18次/分，血压123/82mmHg。两肺呼吸音清，心率64次/分，心律齐，腹软，无压痛，四肢无水肿。生化:葡萄糖：5.2mmol/L。血常规Rbc：4.24×10<sup>12</sup>/L，Hb：123g/L，Wbc：6.2×10<sup>9</sup>/L，中性粒比例：81.4%，淋巴细胞比例：9.6%，Plt：162×10<sup>9</sup>/L。C反应蛋白：2.29MG/L，糖化血红蛋白：5.7%，低密度脂蛋白：1.57mmol/L，甘油三酯：1.05mmol/L，高密度脂蛋白：2.22mmol/L，总胆固醇：3.84mmol/L。Cl：107mmol/L，K：4.3mmol/L，Na：142mmol/L，A/G：1.4，白蛋白：42g/L，谷丙转氨酶：35u/L，间接胆红素：8umol/L，碱性磷酸酶（ALP）：58u/L，总胆红素：9umol/L，总蛋白：72g/L。球蛋白：30g/L。直接胆红素：1umol/L。肌酐：73umol/L，尿素氮：7.4mmol/L，尿酸：300umol/L（，白细胞：4u/L，尿胆原：正常，葡萄糖：（-）mmol/L，隐血：（-）。心电图：心肌缺血，窦性心动过缓。B超：左肝实质低回声结节，良性机会大，胆囊内壁隆起性病变，息肉可能，右肾囊肿，左肾结石（2025.6.5上海健康医学院崇明分院）CT：两侧放射冠区腔隙灶，副鼻窦粘膜略厚，两肺数枚粟粒灶，两肺门及纵膈多发钙化灶，两侧部分肋骨扭曲（2025.6.5上海健康医学院崇明分院）')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'669', N'1391', N'其他', N'基本可靠', N'胡新志', N'2025-06-19 00:00:00.000', NULL, NULL, N'上海市第五福利院', N'', N'宋民胜，男，60岁，智力低下60年。患者自幼智力低下，日常生活不能自理，目前洗浴、穿脱衣、洗漱、进食、行走、如厕等需他人完全帮助。入院体检提示：心动过缓、腔隙性脑梗死、心肌供血不足。患者目前精神状态可，食欲、大小便、睡眠较正常。有乙肝病史。体检：体温 36.7℃，脉搏60次/分，呼吸18次/分，血压96/65mmHg。两肺呼吸音粗，心率60次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，四肢无水肿，双下肢肌肉萎缩，肌张力亢进，无偏瘫,左上肢肌力为4级,左下肢肌力为0级,右上肢肌力为4级,右下肢肌力为0级。生化:葡萄糖：4.8mmol/L。糖化血红蛋白：5.5%，低密度脂蛋白：2.87mmol/L，甘油三酯：0.84mmol/L，高密度脂蛋白：2.20mmol/L，总胆固醇：5.20mmol/L。Ca：2.04mmol/L，Cl：105mmol/L，K：4.3mmol/L，Na：140mmol/L，A/G：1.13，白蛋白：36g/L，谷丙转氨酶：11u/L，间接胆红素：5umol/L，碱性磷酸酶（ALP）：123u/L，总胆红素：6umol/L，总蛋白：68g/L。球蛋白：32g/L。直接胆红素：1umol/L。肌酐：66umol/L，尿素氮：6.0mmol/L，尿酸：0.22umol/L，白细胞：177u/L，酸碱度：5.5，上皮细胞：5u/L，亚硝酸盐：+，隐血：+。心电图：窦性心动过缓 T波改变。B超：1.右肝实质高回声结节，血管瘤可能2.慢性胆囊炎、胆囊结石。CT：1.左侧基底节区、两侧放射冠区腔隙灶、老年脑改变2.副鼻窦粘膜略厚、两侧眼球萎缩伴钙化3.两肺少许条索灶、右肺下叶粟粒灶4.右侧部分肋骨扭曲、胸12椎体密度欠均匀5.冠状动脉少许钙化6.脾脏钙化灶。')
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'671', N'1393', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-07-28 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'673', N'1395', N'患者本人及家属', N'基本可信', N'朱晓霞', N'2025-08-26 00:00:00.000', NULL, NULL, NULL, NULL, NULL)
GO

INSERT INTO [dbo].[MRTotal] ([RecordId], [ElderId], [Talker], [BealiveCondition], [Doctor], [RecordDate], [IllName], [IllItemName], [Talker2], [helpDoctor], [summary]) VALUES (N'672', N'1394', N'患者家属', N'基本可靠', N'张丁', N'2025-08-25 00:00:00.000', NULL, NULL, N'', N'', N'唐修海，男，80岁，记忆力进行性减退伴行为改变5年。家属代诉患者5年前开始出现记忆力进行性减退，丢三落四、易忘事，总觉得自己东西被偷，时有藏东西行为，无猜疑、被害妄想。2024年曾至上海市第十人民医院就诊，诊断为“阿尔茨海默症（混合型）”。予以多奈哌齐治疗，症状较前改善。目前口服多奈哌齐、乌灵胶囊。发病以来患者无发热，无胸闷气急，无呕吐等。本次入院体检提示血糖偏高、肾功能不全、胆囊结石、右肺结节。患者目前情绪较稳定，无明显吵闹、纠缠不清，饮食可，夜间睡眠差，目前服用右佐匹克隆改善睡眠。体检：体温 36.7℃，脉搏62次/分，呼吸19次/分，血压130/84mmHg。两肺呼吸音粗，心率62次/分，心律齐，无腹壁紧张，无压痛，无反跳痛，无液波震颤，无肿块。四肢无水肿，无肌肉萎缩，肌张力无异常，无偏瘫,左上肢肌力为5级,左下肢肌力为5级,右上肢肌力为5级,右下肢肌力为5级。生化:葡萄糖：8.97mmol/L。Rbc：4.92×10<sup>12</sup>/L，Hb：136g/L，Wbc：6.87×10<sup>9</sup>/L，中性粒比例：59.2%，淋巴细胞比例：29%，Plt：161×10<sup>9</sup>/L（2025.8.14上海交通大学医学院附属松江医院）。糖化血红蛋白：5.4%，低密度脂蛋白：3.49mmol/L，甘油三酯：1.66mmol/L，高密度脂蛋白：1.44mmol/L，总胆固醇：5.14mmol/L。Ca：2.42mmol/L，Cl：105.58mmol/L，K：4.12mmol/L，Na：140.7mmol/L，P：0.88mmol/L。A/G：1.82，白蛋白：46.52g/L，谷丙转氨酶：10.29u/L，间接胆红素：5.67umol/L，碱性磷酸酶（ALP）：67.78u/L，总胆红素：8.86umol/L，总蛋白：72.09g/L。球蛋白：25.57g/L。直接胆红素：3.19umol/L。肌酐：111.06umol/L，尿素氮：8.76mmol/L，尿酸：375.52umol/L（2025.8.14上海交通大学医学院附属松江医院），白细胞：-u/L，尿胆原：-，葡萄糖：-mmol/L，酸碱度：5.5，酮体：-，亚硝酸盐：-，隐血：-。心电图：正常心电图。X线胸片：(缺)。B超：胆囊泥沙样结石，右肾小囊肿，肝脏、胰腺、脾脏、左肾未见明显占位。CT：颅脑CT:老年脑改变。
胸部CT：1.右肺上叶尖段磨玻璃结节（5mm*4mm）。2.主动脉、冠状动脉硬化。')
GO

SET IDENTITY_INSERT [dbo].[MRTotal] OFF
GO


-- ----------------------------
-- Primary Key structure for table MRTotal
-- ----------------------------
ALTER TABLE [dbo].[MRTotal] ADD CONSTRAINT [PK_MRTotal] PRIMARY KEY NONCLUSTERED ([RecordId])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

