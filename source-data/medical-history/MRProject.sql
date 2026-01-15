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

 Date: 07/01/2026 17:39:41
*/


-- ----------------------------
-- Table structure for MRProject
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[MRProject]') AND type IN ('U'))
	DROP TABLE [dbo].[MRProject]
GO

CREATE TABLE [dbo].[MRProject] (
  [MRProjectId] int  IDENTITY(1,1) NOT NULL,
  [HProjectName] varchar(20) COLLATE Chinese_PRC_CI_AS  NULL,
  [PSortId] int  NULL,
  [isIllProject] bit  NULL,
  [FrontContent] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [PlaceContent] varchar(10) COLLATE Chinese_PRC_CI_AS  NULL,
  [BehindContent] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [HProjectView] varchar(10) COLLATE Chinese_PRC_CI_AS  NULL
)
GO

ALTER TABLE [dbo].[MRProject] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of MRProject
-- ----------------------------
SET IDENTITY_INSERT [dbo].[MRProject] ON
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'1', N'T', N'25', N'0', N'', N'后', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'2', N'P', N'25', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'3', N'R', N'25', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'4', N'Bp', N'25', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'8', N'神志', N'26', N'1', N'神志', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'10', N'发育', N'26', N'1', N'发育', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'11', N'营养', N'26', N'1', N'营养', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'12', N'体位', N'26', N'1', N'', N'后', N'体位', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'13', N'面容与表情', N'26', N'1', N'表情', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'14', N'体检', N'26', N'1', N'体检', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'15', N'颜色', N'27', N'1', N'皮肤颜色', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'16', N'温度', N'27', N'0', N'', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'17', N'湿度', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'18', N'弹性', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'19', N'水肿', N'27', N'1', N'', N'中', N'水肿', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'20', N'皮疹', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'21', N'瘀点', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'22', N'紫癜', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'23', N'皮下结节', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'25', N'肿块', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'26', N'蜘蛛痣', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'27', N'肝掌', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'28', N'溃疡', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'29', N'瘢痕', N'27', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'31', N'头颅', N'29', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'32', N'眼', N'29', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'33', N'耳', N'29', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'34', N'鼻', N'29', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'35', N'口腔', N'29', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'36', N'两侧', N'30', N'1', N'两侧', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'37', N'强直', N'30', N'0', N'', N'后', N'强直', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'38', N'颈静脉怒张', N'30', N'1', N'', N'后', N'颈静脉怒张', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'39', N'肝－颈静脉返流征', N'30', N'1', N'', N'后', N'肝－颈静脉返流征', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'40', N'颈动脉异常搏动', N'30', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'41', N'气管位置', N'30', N'1', N'气管', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'43', N'望诊', N'31', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'44', N'触诊', N'31', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'45', N'叩诊', N'31', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'46', N'听诊', N'31', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'51', N'毛细血管搏动', N'46', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'52', N'枪击音', N'46', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'53', N'水冲脉', N'46', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'54', N'望诊', N'47', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'55', N'触诊', N'47', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'56', N'叩诊', N'47', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'57', N'听诊', N'47', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'58', N'活动度', N'49', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'59', N'畸形', N'49', N'1', N'脊柱', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'60', N'压痛', N'49', N'1', N'', N'后', N'压痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'61', N'叩击痛', N'49', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'62', N'畸形', N'50', N'1', N'', N'后', N'畸形', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'63', N'杵状指', N'50', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'64', N'静脉曲张', N'50', N'1', N'', N'后', N'静脉曲张', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'65', N'骨折', N'50', N'1', N'', N'后', N'骨折', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'69', N'水肿', N'50', N'1', N'四肢', N'中', N'水肿', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'70', N'肌肉萎缩', N'50', N'1', N'四肢', N'中', N'肌肉萎缩', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'71', N'肌张力', N'50', N'1', N'肌张力', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'72', N'偏瘫', N'50', N'1', N'', N'后', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'88', N'肿大', N'28', N'1', N'全身浅表淋巴结', N'后', N'肿大', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'90', N'动脉异常搏动', N'46', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'91', N'未检', N'48', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'93', N'药物', N'7', N'0', N'', N'后', N'过敏史', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'94', N'食物', N'7', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'96', N'出生地', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'97', N'常驻', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'98', N'血吸虫病史', N'5', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'99', N'血吸虫疫水接触史', N'5', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'102', N'吸烟嗜好', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'103', N'喝酒嗜好', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'104', N'麻醉药品或毒品使用', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'105', N'职业', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'106', N'学历', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'109', N'冶游史', N'6', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'111', N'大小', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'112', N'形状', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'113', N'肿块', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'114', N'压痛', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'115', N'瘢痕', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'116', N'头发分布', N'59', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'117', N'眉毛', N'60', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'118', N'睫毛', N'60', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'119', N'眼睑', N'60', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'120', N'眼球', N'60', N'1', N'', N'前', N'眼球', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'121', N'结膜', N'60', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'122', N'巩膜', N'60', N'1', N'巩膜', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'123', N'角膜', N'60', N'1', N'角膜', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'124', N'瞳孔', N'60', N'1', N'瞳孔', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'125', N'畸形', N'61', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'126', N'分泌物', N'61', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'127', N'乳突压痛', N'61', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'129', N'左耳听力', N'61', N'0', N'左耳听力', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'130', N'右耳听力', N'61', N'0', N'右耳听力', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'131', N'畸形', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'132', N'鼻翼扇动', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'133', N'分泌物', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'134', N'出血', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'135', N'阻塞', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'136', N'鼻中隔偏曲', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'137', N'鼻窦压痛', N'62', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'138', N'张口呼吸', N'63', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'139', N'畸形', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'140', N'颜色', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'141', N'疱疹', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'142', N'皲裂', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'143', N'溃疡', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'144', N'色素沉着', N'65', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'145', N'龃牙', N'67', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'146', N'缺牙', N'67', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'147', N'义牙', N'67', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'148', N'残根', N'67', N'1', NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'150', N'色泽', N'68', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'151', N'肿胀', N'68', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'152', N'溃疡', N'68', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'153', N'溢脓', N'68', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'154', N'出血', N'68', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'155', N'形态', N'69', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'157', N'舌苔', N'69', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'158', N'溃疡', N'69', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'159', N'运动', N'69', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'160', N'震颤', N'69', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'161', N'偏斜', N'69', N'1', N'伸舌', N'中', N'偏斜', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'162', N'色泽', N'70', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'164', N'分泌物', N'70', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'165', N'反射', N'70', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'166', N'悬雍垂', N'70', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'167', N'大小', N'71', N'1', N'扁桃体', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'168', N'充血', N'71', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'169', N'分泌物', N'71', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'170', N'假膜', N'71', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'171', N'发音', N'72', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'172', N'大小', N'73', N'1', N'', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'173', N'硬度', N'73', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'174', N'压痛', N'73', N'1', N'', N'后', N'压痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'175', N'结节', N'73', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'176', N'震颤', N'73', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'177', N'血管杂音', N'73', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'178', N'两侧', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'179', N'畸形', N'74', N'1', N'胸廓', N'后', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'180', N'局部隆起或凹陷', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'181', N'压痛', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'182', N'乳房大小', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'183', N'乳房肿块', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'184', N'乳房压痛', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'186', N'胸壁静脉曲张', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'187', N'皮下气肿', N'74', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'188', N'呼吸运动', N'75', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'189', N'呼吸类型', N'75', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'190', N'肋间隙', N'75', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'192', N'呼吸活动度', N'76', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'193', N'语颤', N'76', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'194', N'胸膜摩擦感', N'76', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'195', N'皮下捻发感', N'76', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'196', N'呼吸音', N'78', N'1', N'两肺呼吸音', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'197', N'干啰音', N'78', N'1', N'', N'后', N'干啰音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'198', N'湿啰音', N'78', N'1', N'', N'后', N'湿啰音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'199', N'胸膜摩擦音', N'78', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'200', N'语音传导', N'78', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'201', N'叩诊音', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'202', N'异常呼吸音', N'78', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'203', N'心前区隆起', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'204', N'心尖搏动', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'205', N'心尖搏动强度', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'206', N'搏动', N'80', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'207', N'震颤', N'80', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'208', N'心包摩擦感', N'80', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'209', N'心脏相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'217', N'两侧', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'218', N'腹部形状', N'84', N'1', N'腹部', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'219', N'胃肠蠕动波', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'220', N'皮疹', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'221', N'色素', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'222', N'条纹', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'223', N'瘢痕', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'224', N'静脉曲张', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'225', N'局部隆起或凹陷', N'84', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'226', N'腹壁紧张', N'85', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'227', N'压痛', N'85', N'1', N'', N'后', N'压痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'228', N'反跳痛', N'85', N'1', N'', N'后', N'反跳痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'229', N'液波震颤', N'85', N'0', N'', N'后', N'液波震颤', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'230', N'肿块', N'85', N'1', N'', N'后', N'肿块', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'231', N'大小', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'232', N'质地', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'233', N'表面', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'234', N'边缘', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'235', N'结节', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'236', N'压痛', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'237', N'搏动', N'86', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'239', N'压痛', N'87', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'240', N'Murphy征', N'87', N'1', N'Murphy征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'241', N'外形', N'88', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'242', N'压痛', N'88', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'243', N'外形', N'89', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'244', N'压痛', N'89', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'245', N'叩击痛', N'89', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'246', N'肝浊音界', N'91', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'247', N'肝区', N'91', N'1', N'肝区', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'248', N'移动性浊音', N'91', N'1', N'', N'后', N'移动性浊音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'249', N'叩诊音', N'91', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'250', N'肾区叩击痛', N'91', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'251', N'肠鸣音', N'92', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'252', N'振水音', N'92', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'253', N'血管杂音', N'92', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'254', N'角膜反射', N'94', N'1', N'角膜反射', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'255', N'腹壁反射', N'93', N'1', N'腹壁反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'256', N'提睾反射', N'93', N'0', N'提睾反射', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'257', N'肱二头肌反射', N'93', N'1', N'肱二头肌反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'258', N'肱三头肌反射', N'93', N'1', N'肱三头肌反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'259', N'膝腱反射', N'93', N'1', N'膝腱反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'260', N'跟腱反射', N'93', N'1', N'跟腱反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'261', N'Babinski征', N'94', N'1', N'Babinski征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'262', N'Oppenheim征', N'94', N'1', N'Oppenheim征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'263', N'Gordon征', N'94', N'1', N'Gordon征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'264', N'Chaddock征', N'94', N'1', N'Chaddock征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'265', N'Hoffmann征', N'94', N'1', N'Hoffmann征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'266', N'颈项强直', N'95', N'1', N'颈项强直', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'267', N'Kering征', N'95', N'1', N'Kering征', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'268', N'Brudzinski', N'95', N'1', N'Brudzinski', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'270', N'主诉', N'1', N'0', N'', N'', N'', N'大')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'271', N'现病史', N'2', N'0', N'', N'', N'', N'大')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'272', N'既往史', N'5', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'275', N'家族遗传性病史', N'9', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'276', N'对光反射', N'60', N'1', N'对光反射', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'279', N'肺下界右锁骨中线', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'280', N'肺下界右肩胛线', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'281', N'肺下界左肩胛线', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'282', N'右侧肺下界移动度', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'283', N'抬举样心尖搏动', N'80', N'0', N'', N'后', N'抬举样心尖搏动', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'285', N'右二肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'286', N'右三肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'287', N'右四肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'288', N'左二肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'289', N'左三肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'290', N'左四肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'291', N'左五肋间相对浊音界', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'292', N'锁中线距正中线距离', N'81', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'293', N'心率', N'82', N'1', N'心率', N'中', N'次/分', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'294', N'心律', N'82', N'1', N'心律', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'295', N'第一心音', N'82', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'296', N'各瓣膜区杂音', N'82', N'1', N'', N'后', N'杂音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'297', N'心包摩擦音', N'82', N'1', N'', N'后', N'心包摩擦音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'298', N'肺下界左锁骨中线', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'299', N'左侧肺下界移动度', N'77', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'302', N'左上肢肌力', N'50', N'1', N'左上肢肌力', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'303', N'左下肢肌力', N'50', N'1', N'左下肢肌力', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'304', N'右上肢肌力', N'50', N'1', N'右上肢肌力', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'305', N'右下肢肌力', N'50', N'1', N'右下肢肌力', N'前', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'307', N'褥疮', N'27', N'1', N'', N'后', N'褥疮', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'309', N'压痛', N'28', N'1', N'', N'后', N'压痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'310', N'溃疡', N'28', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'314', N'心尖搏动位于', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'315', N'心尖搏动范围直径', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'316', N'锁骨中线', N'79', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'317', N'早搏', N'82', N'1', N'', N'后', N'早搏', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'319', N'葡萄糖/mmol/L', N'52', N'0', N'', N'后', N'mmol/L', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'320', N'尿素氮', N'97', N'0', N'', N'后', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'321', N'肌酐', N'97', N'0', N'', N'后', N'μmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'322', N'尿酸', N'97', N'0', N'', N'后', N'μmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'323', N'谷丙转氨酶', N'102', N'0', N'', N'后', N'U/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'329', N'HBsAg', N'103', N'0', N'HBsAg', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'330', N'HBeAg', N'103', N'0', N'HBeAg', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'331', N'抗HBc', N'103', N'0', N'抗HBc', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'332', N'抗HBe', N'103', N'0', N'抗HBe', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'333', N'抗HBs', N'103', N'0', N'抗HBs', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'334', N'抗HBc-IgM', N'103', N'0', N'抗HBc-IgM', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'335', N'反跳痛', N'87', N'1', N'', N'后', N'反跳痛', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'336', N'哮鸣音', N'78', N'1', N'', N'后', N'哮鸣音', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'337', N'心电图', N'52', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'338', N'X线胸片', N'52', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'339', N'B超', N'52', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'340', N'CT', N'52', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'341', N'其他1', N'52', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'376', N'其他', N'6', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'377', N'其他', N'7', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'379', N'其他', N'9', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'381', N'其他', N'27', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'382', N'其他', N'28', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'383', N'其他', N'30', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'384', N'其他', N'46', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'385', N'其他', N'48', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'386', N'其他', N'49', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'387', N'其他', N'50', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'389', N'其他', N'59', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'390', N'其他', N'60', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'391', N'其他', N'61', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'392', N'其他', N'62', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'393', N'其他', N'63', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'394', N'其他', N'65', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'395', N'其他', N'68', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'396', N'其他', N'69', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'397', N'其他', N'70', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'398', N'其他', N'71', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'399', N'其他', N'72', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'401', N'其他', N'74', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'402', N'其他', N'75', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'403', N'其他', N'76', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'404', N'其他', N'77', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'405', N'其他', N'78', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'406', N'其他', N'79', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'407', N'其他', N'80', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'408', N'其他', N'81', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'409', N'其他', N'82', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'410', N'其他', N'84', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'411', N'其他', N'85', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'412', N'其他', N'86', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'413', N'其他', N'87', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'414', N'其他', N'88', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'415', N'其他', N'89', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'416', N'其他', N'91', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'417', N'其他', N'92', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'418', N'其他', N'93', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'419', N'其他', N'94', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'420', N'其他', N'95', N'1', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'421', N'其他', N'26', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'422', N'其他', N'73', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'424', N'1、', N'54', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'425', N'2、', N'54', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'437', N'高血压病', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'438', N'冠心病', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'439', N'糖尿病', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'440', N'脑梗塞', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'441', N'脑出血', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'445', N'胆囊炎', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'446', N'胆囊结石', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'451', N'控制，监测血压。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'452', N'护心、保心治疗。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'453', N'控制、监测血糖。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'454', N'活血、化淤。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'455', N'抗感染治疗。', N'55', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'456', N'止咳、化痰。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'457', N'醒脑、营养脑组织。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'458', N'抗癫痫。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'459', N'抗骨质疏松。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'460', N'控制、监测血脂。', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'461', N'改善、监测肾功能。', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'462', N'适当肢体功能锻炼。', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'463', N'清淡饮食。', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'464', N'糖尿病饮食。', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'465', N'老年性痴呆症', N'56', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'466', N'血管性痴呆', N'56', N'0', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'468', N'骨折后后遗症：', N'56', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'469', N'骨质疏松症', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'470', N'帕金森氏病', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'471', N'肾功能不全', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'472', N'动脉硬化症', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'473', N'褥疮', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'474', N'支气管扩张', N'56', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'475', N'胃溃疡', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'476', N'十二指肠溃疡', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'479', N'肾炎', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'480', N'甲状腺功能亢进', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'481', N'甲状腺功能减退', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'482', N'贫血', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'483', N'陈旧性肺结核', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'484', N'癌症：', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'485', N'血液病', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'486', N'心律失常', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'487', N'房室传导阻滞', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'489', N'其他', N'55', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'490', N'腹股沟斜疝', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'491', N'其他1', N'56', N'0', N'', N'', N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'492', N'其他2', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'493', N'其他3', N'56', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'494', N'其他2', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'495', N'其他3', N'55', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'496', N'婚育史', N'8', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'497', N'其他2', N'52', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'498', N'其他3', N'52', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'499', N'其他4', N'52', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'500', N'其他5', N'52', NULL, NULL, NULL, N'', NULL)
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'501', N'胃炎', N'56', N'0', N'', N'', N'', N'大')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'502', N'慢性阻塞性肺疾病', N'56', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'503', N'阻塞性肺气肿', N'56', N'0', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'504', N'类风湿性关节炎', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'505', N'尿路感染', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'506', N'咽炎', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'507', N'肋骨骨折', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'508', N'股骨颈骨折后遗症', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'509', N'股骨粗隆间骨折', N'56', N'1', N'', N'前', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'511', N'神经性病变', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'512', N'尿潴留', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'513', N'腰椎间盘突出', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'514', N'肾绞痛', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'516', N'肩周炎', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'517', N'湿疹', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'518', N'血小板减少症', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'519', N'肺占位', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'520', N'老年性精神障碍', N'56', NULL, NULL, NULL, N'', N'大')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'521', N'肺不张', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'522', N'颈椎病', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'523', N'颈椎病', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'524', N'急性支气管炎', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'525', N'消化道出血', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'526', N'甲沟炎', N'56', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'527', N'手术外伤史', N'5', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'528', N'输血史', N'5', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'529', N'其他', N'5', NULL, NULL, NULL, N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'530', N'糖耐量异常', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'532', N'精神发育迟滞', N'56', N'0', N'', N'前', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'533', N'哑巴', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'534', N'Rbc', N'96', N'0', N'', N'', N'x10^12/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'535', N'Hb', N'96', N'0', N'', N'', N'g/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'536', N'Wbc', N'96', N'0', N'', N'', N'x10^9/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'537', N'中性粒比例', N'96', N'0', N'', N'', N'%', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'538', N'淋巴细胞比例', N'96', N'0', N'', N'', N'%', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'539', N'Plt', N'96', N'0', N'', N'', N'x10^9/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'543', N'甘油三酯', N'100', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'544', N'总胆固醇', N'100', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'545', N'高密度脂蛋白', N'100', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'546', N'低密度脂蛋白', N'100', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'547', N'K', N'98', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'548', N'Na', N'98', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'549', N'Cl', N'98', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'550', N'Ca', N'98', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'551', N'P', N'98', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'552', N'c反应蛋白', N'99', N'0', N'', N'', N'mg/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'553', N'糖化血红蛋白', N'101', N'0', N'', N'', N'%', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'555', N'总蛋白', N'102', N'0', N'', N'', N'g/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'556', N'白蛋白', N'102', N'0', N'', N'', N'g/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'557', N'球蛋白', N'102', N'0', N'', N'', N'g/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'558', N'A/G', N'102', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'559', N'总胆红素', N'102', N'0', N'', N'', N'μmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'560', N'直接胆红素', N'102', N'0', N'', N'', N'μmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'561', N'间接胆红素', N'102', N'0', N'', N'', N'μmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'562', N'碱性磷酸酶（ALP）', N'102', N'0', N'', N'', N'U/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'563', N'餐后2h血糖', N'52', N'0', N'', N'', N'mmol/L', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'564', N'尿胆原', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'565', N'隐血', N'104', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'566', N'酮体', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'567', N'葡萄糖', N'104', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'568', N'蛋白质', N'104', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'569', N'酸碱度', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'570', N'亚硝酸盐', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'571', N'红细胞', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'572', N'白细胞', N'104', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'573', N'管型', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'574', N'上皮细胞', N'104', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'575', N'隐血', N'105', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'576', N'婚姻', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'577', N'配偶', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'578', N'夫妻关系', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'579', N'月经初潮', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'580', N'绝经年龄', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'581', N'子女人数', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'582', N'儿子数目', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'583', N'女儿数目', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'584', N'子女关系', N'8', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'585', N'进食', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'586', N'个人卫生', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'587', N'行走', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'588', N'上下床', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'589', N'饮食', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'590', N'食欲', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'591', N'排便', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'592', N'排便次数', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'593', N'排尿', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'594', N'睡眠', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'595', N'睡眠小时数', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'596', N'辅助睡眠', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'597', N'午睡', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'598', N'癫痫', N'56', N'1', N'', N'', N'', N'小')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'599', N'助眠药物：', N'106', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'600', N'膀胱结石', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'601', N'肾结石', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'602', N'肾积水', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'604', N'完善相关检验项目：', N'55', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'605', N'定期监测：', N'55', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'606', N'低盐低脂饮食。', N'55', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'607', N'脑瘫', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'608', N'脂肪肝', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'609', N'慢性支气管炎', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'610', N'聋哑', N'56', NULL, NULL, NULL, N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'611', N'便秘', N'56', N'1', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'612', N'其他4', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'613', N'其他5', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'614', N'其他6', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'615', N'其他7', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'616', N'其他8', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'617', N'其他9', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'618', N'其他10', N'56', NULL, NULL, NULL, NULL, N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'621', N'其他11', N'56', N'0', N'', N'', N'', N'')
GO

INSERT INTO [dbo].[MRProject] ([MRProjectId], [HProjectName], [PSortId], [isIllProject], [FrontContent], [PlaceContent], [BehindContent], [HProjectView]) VALUES (N'622', N'会厌溃疡', N'56', N'0', N'', N'', N'', N'')
GO

SET IDENTITY_INSERT [dbo].[MRProject] OFF
GO


-- ----------------------------
-- Primary Key structure for table MRProject
-- ----------------------------
ALTER TABLE [dbo].[MRProject] ADD CONSTRAINT [PK_MRProject] PRIMARY KEY NONCLUSTERED ([MRProjectId])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

