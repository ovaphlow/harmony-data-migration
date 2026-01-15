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

 Date: 07/01/2026 17:40:15
*/


-- ----------------------------
-- Table structure for MRSort
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[MRSort]') AND type IN ('U'))
	DROP TABLE [dbo].[MRSort]
GO

CREATE TABLE [dbo].[MRSort] (
  [MRSortId] int  IDENTITY(1,1) NOT NULL,
  [MRSortName] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [MRSortKind] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [MRSortLargeKind] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [MRSortIndex] int  NULL
)
GO

ALTER TABLE [dbo].[MRSort] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of MRSort
-- ----------------------------
SET IDENTITY_INSERT [dbo].[MRSort] ON
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'1', N'主诉', N'病史', N'', N'1')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'2', N'现病史', N'病史', NULL, N'2')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'5', N'既往史', N'病史', NULL, N'3')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'6', N'个人史', N'病史', NULL, N'4')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'7', N'过敏史', N'病史', NULL, N'5')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'8', N'婚育史', N'病史', NULL, N'6')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'9', N'家族史', N'病史', NULL, N'7')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'25', N'生命体征', N'体格检查', N'一般情况', N'8')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'26', N'一般状况', N'体格检查', N'一般情况', N'9')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'27', N'皮肤,粘膜', N'体格检查', N'一般情况', N'10')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'28', N'全身浅表淋巴结', N'体格检查', N'一般情况', N'11')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'30', N'颈部', N'体格检查', N'颈部', N'23')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'46', N'周围血管征', N'体格检查', N'心脏', N'34')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'48', N'肛门、直肠及外生殖器', N'体格检查', N'腹部', N'43')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'49', N'脊柱', N'体格检查', N'脊柱四肢', N'44')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'50', N'四肢', N'体格检查', N'脊柱四肢', N'45')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'52', N'实验室及器械检查', N'实验室及器械', NULL, N'49')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'54', N'鉴别诊断', N'鉴别诊断', N'', N'51')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'55', N'诊疗计划', N'诊疗计划', NULL, N'53')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'56', N'初步诊断', N'初步诊断', N'', N'54')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'57', N'医生签名', N'初步诊断', N'', N'55')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'58', N'日期', NULL, NULL, N'56')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'59', N'头颅', N'体格检查', N'头颅及器官', N'12')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'60', N'眼', N'体格检查', N'头颅及器官', N'13')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'61', N'耳', N'体格检查', N'头颅及器官', N'14')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'62', N'鼻', N'体格检查', N'头颅及器官', N'15')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'63', N'口腔', N'体格检查', N'头颅及器官', N'16')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'65', N'唇', N'体格检查', N'头颅及器官', N'17')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'68', N'牙龈', N'体格检查', N'头颅及器官', N'18')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'69', N'舌', N'体格检查', N'头颅及器官', N'19')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'70', N'咽', N'体格检查', N'头颅及器官', N'20')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'71', N'扁桃体', N'体格检查', N'头颅及器官', N'21')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'72', N'喉', N'体格检查', N'头颅及器官', N'22')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'73', N'甲状腺', N'体格检查', N'颈部', N'24')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'74', N'胸廓', N'体格检查', N'肺部', N'25')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'75', N'望诊(肺)', N'体格检查', N'肺部', N'26')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'76', N'触诊(肺)', N'体格检查', N'肺部', N'27')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'77', N'叩诊(肺)', N'体格检查', N'肺部', N'28')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'78', N'听诊(肺)', N'体格检查', N'肺部', N'29')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'79', N'望诊(心)', N'体格检查', N'心脏', N'30')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'80', N'触诊(心)', N'体格检查', N'心脏', N'31')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'81', N'叩诊(心)', N'体格检查', N'心脏', N'32')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'82', N'听诊(心)', N'体格检查', N'心脏', N'33')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'84', N'望诊(腹)', N'体格检查', N'腹部', N'35')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'85', N'触诊(腹)', N'体格检查', N'腹部', N'36')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'86', N'肝脏', N'体格检查', N'腹部', N'37')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'87', N'胆囊', N'体格检查', N'腹部', N'38')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'88', N'脾脏', N'体格检查', N'腹部', N'39')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'89', N'肾脏', N'体格检查', N'腹部', N'40')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'91', N'叩诊(腹)', N'体格检查', N'腹部', N'41')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'92', N'听诊(腹)', N'体格检查', N'腹部', N'42')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'93', N'生理反射', N'体格检查', N'神经系统', N'46')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'94', N'病理反射', N'体格检查', N'神经系统', N'47')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'95', N'脑膜刺激征', N'体格检查', N'神经系统', N'48')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'96', N'血常规', N'实验室及器械', N'实验室及器械', N'57')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'97', N'肾功能', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'98', N'电解质', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'99', N'c反应蛋白', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'100', N'血脂', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'101', N'糖化血红蛋白', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'102', N'肝功能', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'103', N'乙肝二对半', N'实验室及器械', N'实验室及器械', N'58')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'104', N'尿常规', N'实验室及器械', N'', N'59')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'105', N'粪常规', N'实验室及器械', N'实验室及器械', N'59')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'106', N'自理能力', N'病史', N'病史', N'59')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'108', N'膀胱结石', N'初步诊断', N'初步诊断', N'60')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'109', N'肾结石', N'初步诊断', N'初步诊断', N'61')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'110', N'肾积水', N'初步诊断', N'初步诊断', N'62')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'111', N'耻骨上膀胱切开取石术后', N'初步诊断', N'初步诊断', N'63')
GO

INSERT INTO [dbo].[MRSort] ([MRSortId], [MRSortName], [MRSortKind], [MRSortLargeKind], [MRSortIndex]) VALUES (N'112', N'高脂血症', N'初步诊断', N'初步诊断', N'64')
GO

SET IDENTITY_INSERT [dbo].[MRSort] OFF
GO


-- ----------------------------
-- Primary Key structure for table MRSort
-- ----------------------------
ALTER TABLE [dbo].[MRSort] ADD CONSTRAINT [PK_MRSort] PRIMARY KEY NONCLUSTERED ([MRSortId])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

