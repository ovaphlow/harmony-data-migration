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

 Date: 07/01/2026 17:39:51
*/


-- ----------------------------
-- Table structure for MRProjectDetail
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[MRProjectDetail]') AND type IN ('U'))
	DROP TABLE [dbo].[MRProjectDetail]
GO

CREATE TABLE [dbo].[MRProjectDetail] (
  [MRSortDetailId] int  IDENTITY(1,1) NOT NULL,
  [MRProjectId] int  NULL,
  [IllName] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [IllDescribe] varchar(1000) COLLATE Chinese_PRC_CI_AS  NULL
)
GO

ALTER TABLE [dbo].[MRProjectDetail] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Primary Key structure for table MRProjectDetail
-- ----------------------------
ALTER TABLE [dbo].[MRProjectDetail] ADD CONSTRAINT [PK_MRProjectDetail] PRIMARY KEY NONCLUSTERED ([MRSortDetailId])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

