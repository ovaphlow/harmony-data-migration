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

 Date: 07/01/2026 17:40:01
*/


-- ----------------------------
-- Table structure for MRProjectReference
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[MRProjectReference]') AND type IN ('U'))
	DROP TABLE [dbo].[MRProjectReference]
GO

CREATE TABLE [dbo].[MRProjectReference] (
  [MRReferenceId] int  IDENTITY(1,1) NOT NULL,
  [MRReferenceName] varchar(30) COLLATE Chinese_PRC_CI_AS  NULL,
  [HProjectId] int  NULL,
  [IsFirst] char(1) COLLATE Chinese_PRC_CI_AS  NULL
)
GO

ALTER TABLE [dbo].[MRProjectReference] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of MRProjectReference
-- ----------------------------
SET IDENTITY_INSERT [dbo].[MRProjectReference] ON
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'1', N'清晰', N'8', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'2', N'淡漠', N'8', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'3', N'模糊', N'8', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'4', N'昏睡', N'8', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'5', N'谵妄', N'8', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'6', N'昏迷', N'8', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'7', N'正常', N'10', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'8', N'异常', N'10', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'9', N'良好', N'11', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'10', N'中等', N'11', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'11', N'不良', N'11', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'12', N'肥胖', N'11', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'13', N'自动', N'12', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'14', N'被动', N'12', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'15', N'强迫', N'12', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'16', N'安静', N'13', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'17', N'忧虑', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'18', N'烦躁', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'19', N'痛苦', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'20', N'为急性病容', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'21', N'为慢性病容', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'22', N'为特殊面容', N'13', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'23', N'合作', N'14', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'24', N'不合作', N'14', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'25', N'无异常', N'15', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'26', N'潮红', N'15', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'27', N'苍白', N'15', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'28', N'发绀', N'15', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'29', N'黄染', N'15', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'30', N'色素沉着', N'15', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'31', N'适中', N'16', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'32', N'偏高', N'16', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'33', N'偏低', N'16', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'34', N'适中', N'17', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'35', N'偏高', N'17', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'36', N'偏低(干燥)', N'17', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'37', N'良好', N'18', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'38', N'减弱', N'18', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'39', N'差', N'18', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'40', N'有', N'19', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'41', N'无', N'19', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'42', N'有', N'20', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'43', N'无', N'20', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'44', N'有', N'21', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'45', N'无', N'21', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'46', N'有', N'22', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'47', N'无', N'22', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'48', N'有', N'23', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'49', N'无', N'23', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'50', N'有', N'25', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'51', N'无', N'25', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'52', N'有', N'26', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'53', N'无', N'26', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'54', N'有', N'27', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'55', N'无', N'27', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'56', N'有', N'28', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'57', N'无', N'28', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'58', N'有', N'29', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'59', N'无', N'29', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'60', N'无', N'37', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'61', N'有', N'38', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'62', N'无', N'38', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'63', N'左侧', N'39', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'64', N'右侧', N'39', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'65', N'无', N'39', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'66', N'左侧', N'40', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'67', N'右侧', N'40', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'68', N'无', N'40', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'69', N'居中', N'41', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'70', N'左偏', N'41', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'71', N'右偏', N'41', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'72', N'有', N'51', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'73', N'无', N'51', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'74', N'有', N'52', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'75', N'无', N'52', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'76', N'有', N'53', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'77', N'无', N'53', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'78', N'有', N'90', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'79', N'无', N'90', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'80', N'无异常', N'58', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'81', N'部分受限', N'58', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'82', N'完全受限', N'58', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'83', N'侧弯', N'59', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'84', N'无畸形', N'59', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'85', N'左侧凸', N'59', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'86', N'右侧凸', N'59', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'87', N'前凸', N'59', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'88', N'后凸', N'59', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'89', N'有', N'60', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'90', N'无', N'60', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'91', N'有', N'61', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'92', N'无', N'61', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'93', N'有', N'62', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'94', N'无', N'62', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'95', N'有', N'63', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'96', N'无', N'63', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'97', N'有', N'64', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'98', N'无', N'64', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'99', N'有', N'65', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'100', N'无', N'65', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'101', N'有', N'69', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'102', N'无', N'69', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'103', N'有', N'70', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'104', N'无', N'70', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'105', N'无异常', N'71', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'106', N'增强', N'71', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'107', N'亢进', N'71', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'108', N'减弱', N'71', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'109', N'无', N'72', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'110', N'上海', N'96', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'111', N'无异常', N'111', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'112', N'偏大', N'111', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'113', N'偏小', N'111', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'114', N'无畸形', N'112', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'115', N'畸形', N'112', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'116', N'无', N'113', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'117', N'有', N'113', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'118', N'无', N'114', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'119', N'有', N'114', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'120', N'无', N'115', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'121', N'有', N'115', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'122', N'均匀', N'116', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'123', N'不均匀', N'116', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'124', N'无异常', N'117', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'125', N'脱落', N'117', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'126', N'稀疏', N'117', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'127', N'无倒睫', N'118', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'128', N'有倒睫', N'118', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'129', N'无浮肿', N'119', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'130', N'浮肿', N'119', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'131', N'正常', N'120', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'132', N'不正常', N'120', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'133', N'无异常', N'121', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'134', N'充血', N'121', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'135', N'水肿', N'121', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'136', N'苍白', N'121', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'137', N'出血', N'121', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'138', N'无黄染', N'122', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'139', N'黄染', N'122', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'140', N'无异常', N'123', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'141', N'有云翳', N'123', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'142', N'有白斑', N'123', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'143', N'软化', N'123', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'144', N'有溃疡', N'123', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'145', N'有瘢痕', N'123', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'146', N'两侧对称', N'124', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'147', N'两侧不对称', N'124', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'148', N'有', N'125', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'149', N'无', N'125', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'150', N'无', N'126', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'151', N'有', N'126', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'152', N'有', N'127', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'153', N'无', N'127', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'154', N'无异常', N'129', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'155', N'轻度减弱', N'129', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'156', N'中度减弱', N'129', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'157', N'重度减弱', N'129', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'158', N'无', N'129', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'159', N'无异常', N'130', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'160', N'轻度减弱', N'130', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'161', N'中度减弱', N'130', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'162', N'重度减弱', N'130', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'163', N'无', N'130', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'164', N'无', N'131', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'165', N'有', N'131', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'166', N'无', N'132', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'167', N'有', N'132', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'168', N'无', N'133', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'169', N'有', N'133', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'170', N'无', N'134', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'171', N'有', N'134', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'172', N'无', N'135', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'173', N'有', N'135', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'174', N'无', N'136', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'175', N'有', N'136', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'176', N'无', N'137', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'177', N'有', N'137', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'178', N'无', N'138', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'179', N'有', N'138', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'180', N'无', N'139', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'181', N'有', N'139', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'182', N'红润', N'140', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'183', N'发绀', N'140', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'184', N'苍白', N'140', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'185', N'无', N'141', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'186', N'有', N'141', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'187', N'无', N'142', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'188', N'有', N'142', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'189', N'无', N'143', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'190', N'有', N'143', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'191', N'无', N'144', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'192', N'有', N'144', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'193', N'红润', N'150', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'194', N'暗红', N'150', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'195', N'苍白', N'150', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'196', N'无', N'151', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'197', N'有', N'151', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'198', N'无', N'152', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'199', N'有', N'152', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'200', N'无', N'153', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'201', N'有', N'153', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'202', N'无', N'154', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'203', N'有', N'154', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'204', N'无异常', N'155', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'205', N'无', N'158', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'206', N'有', N'158', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'207', N'无异常', N'159', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'208', N'受限', N'159', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'209', N'无', N'160', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'210', N'有', N'160', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'211', N'无', N'161', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'212', N'左侧', N'161', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'213', N'右侧', N'161', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'214', N'红润', N'162', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'215', N'暗红', N'162', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'216', N'鲜红', N'162', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'217', N'苍白', N'162', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'218', N'无', N'164', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'219', N'有', N'164', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'220', N'存在', N'165', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'221', N'消失', N'165', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'222', N'居中', N'166', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'223', N'左偏', N'166', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'224', N'右偏', N'166', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'225', N'无异常', N'167', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'226', N'I度肿大', N'167', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'227', N'II度肿大', N'167', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'228', N'III度肿大', N'167', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'229', N'无', N'168', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'230', N'轻度', N'168', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'231', N'明显', N'168', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'232', N'无', N'169', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'233', N'少量', N'169', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'234', N'大量', N'169', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'235', N'无', N'170', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'236', N'有', N'170', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'237', N'清晰', N'171', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'238', N'嘶哑', N'171', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'239', N'喘鸣', N'171', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'240', N'失音', N'171', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'241', N'无异常', N'172', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'242', N'I度肿大', N'172', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'243', N'II度肿大', N'172', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'244', N'III度肿大', N'172', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'245', N'无异常', N'173', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'246', N'中等', N'173', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'247', N'硬', N'173', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'248', N'无', N'174', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'249', N'有', N'174', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'250', N'无', N'175', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'251', N'有', N'175', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'252', N'无', N'176', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'253', N'有', N'176', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'254', N'无', N'177', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'255', N'有', N'177', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'256', N'对称', N'178', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'257', N'不对称', N'178', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'258', N'无', N'179', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'259', N'为鸡胸', N'179', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'260', N'为桶状胸', N'179', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'261', N'为扁平胸', N'179', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'262', N'无', N'180', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'263', N'有', N'180', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'264', N'无', N'181', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'265', N'有', N'181', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'266', N'有', N'181', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'267', N'两侧对称', N'182', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'268', N'两侧不对称', N'182', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'269', N'无', N'183', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'270', N'有', N'183', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'271', N'无', N'184', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'272', N'有', N'184', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'273', N'有', N'185', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'274', N'无', N'186', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'275', N'有', N'186', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'276', N'无', N'187', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'277', N'有', N'187', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'278', N'两侧对称', N'188', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'279', N'两侧不对称', N'188', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'280', N'胸式呼吸', N'189', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'281', N'腹式呼吸', N'189', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'282', N'无异常', N'190', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'283', N'增宽', N'190', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'284', N'变窄', N'190', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'285', N'无异常', N'192', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'286', N'两侧不对称', N'192', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'287', N'无异常', N'193', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'288', N'增强', N'193', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'289', N'减弱', N'193', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'290', N'无', N'194', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'291', N'有', N'194', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'292', N'无', N'195', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'293', N'有', N'195', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'294', N'清', N'196', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'295', N'粗', N'196', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'296', N'低', N'196', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'297', N'有', N'197', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'298', N'有', N'198', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'299', N'无', N'199', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'300', N'有', N'199', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'301', N'无异常', N'200', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'302', N'增强', N'200', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'303', N'减弱', N'200', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'304', N'清音', N'201', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'305', N'过清音', N'201', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'306', N'浊音', N'201', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'307', N'实音', N'201', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'308', N'鼓音', N'201', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'309', N'无', N'202', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'310', N'有', N'202', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'311', N'无', N'203', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'312', N'有', N'203', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'313', N'无异常', N'204', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'314', N'呈抬举样', N'204', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'315', N'呈凹陷性', N'204', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'316', N'适中', N'205', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'317', N'增强', N'205', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'318', N'减弱', N'205', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'319', N'强度适中', N'206', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'320', N'增强', N'206', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'321', N'减弱', N'206', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'322', N'无', N'207', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'323', N'有', N'207', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'324', N'无', N'208', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'325', N'有', N'208', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'326', N'对称', N'217', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'327', N'不对称', N'217', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'328', N'平坦', N'218', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'329', N'膨隆', N'218', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'330', N'凹陷', N'218', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'331', N'无', N'219', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'332', N'可见', N'219', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'333', N'无', N'220', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'334', N'可见', N'220', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'335', N'无', N'221', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'336', N'可见', N'221', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'337', N'无', N'222', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'338', N'可见', N'222', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'339', N'无', N'223', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'340', N'可见', N'223', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'341', N'无', N'224', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'342', N'有', N'224', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'343', N'无', N'225', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'344', N'可见', N'225', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'345', N'无', N'226', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'346', N'有', N'226', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'347', N'无', N'227', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'348', N'有', N'227', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'349', N'无', N'228', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'350', N'有', N'228', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'351', N'无', N'229', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'352', N'触及', N'229', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'353', N'无', N'230', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'354', N'可触及', N'230', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'355', N'肋下未及', N'231', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'356', N'增大', N'231', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'357', N'软', N'232', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'358', N'韧', N'232', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'359', N'硬', N'232', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'360', N'光滑', N'233', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'361', N'不光滑', N'233', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'362', N'整齐', N'234', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'363', N'不整齐', N'234', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'364', N'无', N'235', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'365', N'可触及', N'235', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'366', N'无', N'236', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'367', N'有', N'236', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'368', N'无', N'237', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'369', N'有', N'237', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'370', N'无', N'239', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'371', N'有', N'239', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'372', N'(-)', N'240', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'373', N'(+)', N'240', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'374', N'未触及', N'241', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'375', N'增大', N'241', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'376', N'缩小', N'241', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'377', N'无', N'242', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'378', N'有', N'242', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'379', N'饱满', N'241', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'380', N'未触及', N'243', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'381', N'无', N'244', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'382', N'无异常', N'246', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'383', N'增大', N'246', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'384', N'缩小', N'246', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'385', N'无叩击痛', N'247', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'386', N'叩击痛', N'247', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'387', N'无', N'248', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'388', N'有', N'248', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'389', N'为高度鼓音', N'249', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'390', N'为浊音', N'249', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'391', N'为实音', N'249', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'392', N'无', N'250', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'393', N'有', N'250', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'394', N'无异常', N'251', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'395', N'增强', N'251', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'396', N'减弱', N'251', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'397', N'亢进', N'251', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'398', N'消失', N'251', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'399', N'金属音', N'251', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'400', N'无', N'252', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'401', N'有', N'252', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'402', N'无', N'253', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'403', N'有', N'253', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'404', N'左侧肢体', N'72', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'405', N'右侧肢体', N'72', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'406', N'无异常', N'254', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'407', N'增强', N'254', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'408', N'减弱', N'254', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'409', N'消失', N'254', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'410', N'无异常', N'255', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'411', N'增强', N'255', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'412', N'减弱', N'255', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'413', N'消失', N'255', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'414', N'未检', N'256', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'415', N'增强', N'256', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'416', N'减弱', N'256', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'417', N'消失', N'256', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'418', N'未检', N'257', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'419', N'增强', N'257', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'420', N'减弱', N'257', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'421', N'消失', N'257', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'422', N'未检', N'258', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'423', N'增强', N'258', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'424', N'减弱', N'258', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'425', N'消失', N'258', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'426', N'未检', N'259', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'427', N'增强', N'259', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'428', N'减弱', N'259', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'429', N'消失', N'259', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'430', N'未检', N'260', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'431', N'增强', N'260', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'432', N'减弱', N'260', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'433', N'消失', N'260', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'434', N'(-)', N'261', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'435', N'(-)', N'262', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'436', N'(-)', N'263', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'437', N'(-)', N'264', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'438', N'(-)', N'265', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'439', N'(+)', N'261', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'440', N'(+)', N'262', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'441', N'(+)', N'263', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'442', N'(+)', N'264', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'443', N'(+)', N'265', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'444', N'有', N'275', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'445', N'无', N'275', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'446', N'未检', N'276', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'447', N'增强', N'276', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'448', N'减弱', N'276', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'449', N'消失', N'276', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'450', N'对称', N'36', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'451', N'不对称', N'36', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'452', N'第五肋间', N'279', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'453', N'第四肋间', N'279', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'454', N'第七肋间', N'279', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'455', N'第六肋间', N'279', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'456', N'第八肋间', N'280', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'457', N'第九肋间', N'280', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'458', N'第十肋间', N'280', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'459', N'第八肋间', N'281', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'460', N'第九肋间', N'281', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'461', N'第十肋间', N'281', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'462', N'0', N'282', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'463', N'1cm', N'282', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'464', N'2cm', N'282', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'465', N'3cm', N'282', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'466', N'4cm', N'282', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'467', N'无', N'283', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'468', N'有', N'283', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'469', N'2.0cm', N'285', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'470', N'2.5cm', N'285', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'471', N'3.0cm', N'285', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'472', N'3.5cm', N'285', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'473', N'2.0cm', N'286', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'474', N'2.5cm', N'286', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'475', N'3.0cm', N'286', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'476', N'3.5cm', N'286', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'477', N'2.0cm', N'287', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'478', N'2.5cm', N'287', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'479', N'3.0cm', N'287', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'480', N'1.5cm', N'288', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'481', N'2.0cm', N'288', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'482', N'2.5cm', N'288', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'483', N'3.0cm', N'288', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'484', N'3.5cm', N'288', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'485', N'6.5cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'486', N'7.0cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'487', N'7.5cm', N'291', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'488', N'8.0cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'489', N'8.5cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'490', N'9.0cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'491', N'9.5cm', N'291', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'492', N'7.0cm', N'292', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'493', N'7.5cm', N'292', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'494', N'8.0cm', N'292', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'495', N'8.5cm', N'292', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'496', N'9.0cm', N'292', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'497', N'9.5cm', N'292', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'498', N'2.5cm', N'289', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'499', N'3.0cm', N'289', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'500', N'3.5cm', N'289', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'501', N'4.0cm', N'289', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'502', N'4.5cm', N'289', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'503', N'4.5cm', N'290', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'504', N'5.0cm', N'290', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'505', N'5.5cm', N'290', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'506', N'6.0cm', N'290', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'507', N'6.5cm', N'290', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'508', N'55', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'509', N'56', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'510', N'57', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'511', N'58', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'512', N'59', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'513', N'60', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'514', N'61', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'515', N'62', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'516', N'63', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'517', N'64', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'518', N'65', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'519', N'67', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'520', N'68', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'521', N'69', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'522', N'70', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'523', N'71', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'524', N'72', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'525', N'73', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'526', N'74', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'527', N'75', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'528', N'76', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'529', N'77', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'530', N'78', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'531', N'79', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'532', N'80', N'293', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'533', N'81', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'534', N'82', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'895', N'未查', N'262', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'535', N'83', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'536', N'84', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'537', N'85', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'538', N'86', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'539', N'87', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'540', N'88', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'541', N'89', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'542', N'90', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'543', N'91', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'544', N'92', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'545', N'93', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'546', N'94', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'547', N'95', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'548', N'96', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'549', N'97', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'550', N'98', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'551', N'99', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'552', N'100', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'553', N'101', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'554', N'102', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'555', N'103', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'556', N'104', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'557', N'105', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'558', N'106', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'559', N'107', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'560', N'108', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'561', N'109', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'562', N'110', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'563', N'111', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'564', N'112', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'565', N'113', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'566', N'114', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'567', N'115', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'568', N'116', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'569', N'117', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'570', N'118', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'571', N'119', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'572', N'120', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'573', N'121', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'574', N'122', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'575', N'123', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'576', N'124', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'577', N'125', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'578', N'126', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'579', N'127', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'580', N'128', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'581', N'129', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'582', N'130', N'293', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'583', N'齐', N'294', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'584', N'不齐', N'294', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'585', N'无增强或减弱', N'295', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'586', N'增强', N'295', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'587', N'减弱', N'295', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'588', N'无', N'297', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'589', N'有', N'297', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'590', N'(-)', N'266', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'591', N'(+)', N'266', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'592', N'(-)', N'267', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'593', N'(+)', N'267', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'594', N'(-)', N'268', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'595', N'(+)', N'268', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'596', N'3.5cm', N'286', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'597', N'0', N'299', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'598', N'1cm', N'299', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'599', N'2cm', N'299', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'600', N'3cm', N'299', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'601', N'第四肋间', N'298', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'602', N'第五肋间', N'298', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'603', N'第六肋间', N'298', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'604', N'第七肋间', N'298', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'605', N'无扩大或缩小', N'209', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'606', N'扩大', N'209', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'607', N'缩小', N'209', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'608', N'无', N'245', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'609', N'左侧有', N'245', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'610', N'右侧有', N'245', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'611', N'左侧有', N'244', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'612', N'右侧有', N'244', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'613', N'左侧可触及', N'243', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'614', N'右侧可触及', N'243', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'615', N'0级', N'302', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'616', N'1级', N'302', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'617', N'2级', N'302', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'618', N'3级', N'302', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'619', N'4级', N'302', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'620', N'5级', N'302', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'621', N'0级', N'303', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'622', N'1级', N'303', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'623', N'2级', N'303', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'624', N'3级', N'303', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'625', N'4级', N'303', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'626', N'5级', N'303', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'627', N'0级', N'304', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'628', N'1级', N'304', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'629', N'2级', N'304', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'630', N'3级', N'304', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'631', N'4级', N'304', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'632', N'5级', N'304', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'633', N'0级', N'305', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'634', N'1级', N'305', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'635', N'2级', N'305', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'636', N'3级', N'305', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'637', N'4级', N'305', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'638', N'5级', N'305', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'639', N'浙江', N'96', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'640', N'江苏', N'96', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'641', N'安徽', N'96', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'642', N'北京', N'96', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'643', N'上海', N'97', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'644', N'无', N'98', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'645', N'有', N'98', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'646', N'无', N'99', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'647', N'有', N'99', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'648', N'无', N'102', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'649', N'有', N'102', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'650', N'无', N'103', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'651', N'有', N'103', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'652', N'无', N'104', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'653', N'有', N'104', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'654', N'退休干部', N'105', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'655', N'退休工人', N'105', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'656', N'家务', N'105', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'657', N'退休教师', N'105', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'658', N'农民', N'105', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'659', N'文盲', N'106', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'660', N'小学文化', N'106', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'661', N'中学文化', N'106', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'662', N'大学文化', N'106', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'663', N'无', N'109', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'664', N'有', N'109', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'665', N'无', N'93', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'666', N'青霉素', N'93', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'667', N'先锋类药物', N'93', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'668', N'特殊药物', N'93', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'669', N'无', N'94', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'670', N'有', N'94', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'671', N'已婚', N'110', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'672', N'丧偶', N'110', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'673', N'离异', N'110', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'674', N'未婚', N'110', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'675', N'无', N'307', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'676', N'有', N'307', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'677', N'无', N'88', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'678', N'有', N'88', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'679', N'无', N'309', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'680', N'有', N'309', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'681', N'无', N'310', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'682', N'有', N'310', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'683', N'第四肋间', N'314', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'684', N'第五肋间', N'314', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'685', N'第六肋间', N'314', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'686', N'第七肋间', N'314', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'687', N'2.0cm', N'315', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'688', N'2.5cm', N'315', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'689', N'3.0cm', N'315', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'690', N'3.5cm', N'315', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'691', N'4.0cm', N'315', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'692', N'上', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'693', N'内侧0.5cm', N'316', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'694', N'内侧1.0cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'695', N'内侧1.5cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'696', N'外测0.5cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'697', N'外测1.0cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'698', N'外测1.5cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'699', N'外测2.0cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'700', N'外测2.5cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'701', N'外测3.0cm', N'316', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'702', N'两侧不对称', N'120', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'703', N'无异常', N'157', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'704', N'颈项', N'37', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'705', N'无', N'197', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'706', N'无', N'198', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'707', N'为二联律', N'294', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'708', N'为奔马律', N'294', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'709', N'未闻及', N'296', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'710', N'可闻及', N'296', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'711', N'未闻及', N'317', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'712', N'闻及1－2次/分', N'317', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'713', N'闻及3－5次/分', N'317', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'714', N'闻及5－10次/分', N'317', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'715', N'闻及频发', N'317', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'716', N'未检', N'91', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'717', N'无', N'335', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'718', N'有', N'335', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'719', N'无', N'336', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'720', N'有', N'336', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'721', N'(-)', N'329', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'722', N'(+)', N'329', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'723', N'(-)', N'330', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'724', N'(+)', N'330', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'725', N'(-)', N'331', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'726', N'(+)', N'331', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'727', N'(-)', N'332', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'728', N'(+)', N'332', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'729', N'(-)', N'334', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'730', N'(+)', N'334', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'731', N'(-)', N'333', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'732', N'(+)', N'333', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'733', N'未检查', N'331', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'734', N'1个', N'306', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'735', N'2个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'736', N'3个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'737', N'4个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'738', N'5个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'739', N'6个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'740', N'7个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'741', N'8个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'742', N'9个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'743', N'10个', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'744', N'健康', N'378', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'745', N'不健康', N'378', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'746', N'1个', N'447', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'747', N'2个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'748', N'3个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'749', N'4个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'750', N'5个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'751', N'6个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'752', N'7个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'753', N'8个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'754', N'9个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'755', N'10个', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'756', N'健康', N'448', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'757', N'不健康', N'448', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'758', N'I级', N'437', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'759', N'II级', N'437', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'760', N'III级', N'437', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'761', N'未检查', N'329', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'762', N'未检查', N'330', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'763', N'未检查', N'332', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'764', N'未检查', N'333', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'765', N'未检查', N'334', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'766', N'I型', N'439', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'767', N'II型', N'439', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'768', N'', N'440', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'769', N'后遗症', N'440', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'770', N'', N'441', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'771', N'后遗症', N'441', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'772', N'', N'438', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'773', N'心功能I级', N'438', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'774', N'心功能II级', N'438', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'775', N'心功能III级', N'438', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'776', N'心功能IV级', N'438', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'777', N'伴慢性阻塞性肺气肿', N'442', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'778', N'伴慢性肺源性心脏病', N'442', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'779', N'临界', N'437', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'780', N'无', N'306', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'781', N'无', N'447', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'782', N'代偿期', N'471', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'783', N'失代偿期', N'471', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'784', N'衰竭期', N'471', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'785', N'尿毒症期', N'471', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'786', N'I度', N'473', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'787', N'II度', N'473', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'788', N'III度', N'473', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'789', N'急性肾小球肾炎', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'790', N'慢性肾小球肾炎', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'791', N'肾病综合症', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'792', N'糖尿病肾病', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'793', N'高血压肾病', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'794', N'急性肾盂肾炎', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'795', N'慢性肾盂肾炎', N'479', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'796', N'缺铁性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'797', N'巨幼红细胞性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'798', N'增生障碍性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'799', N'慢性疾病性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'800', N'老年性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'801', N'营养不良性贫血', N'482', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'802', N'肝癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'803', N'肺癌', N'484', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'804', N'肾癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'805', N'多发性骨髓瘤', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'806', N'乳腺癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'807', N'膀胱癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'808', N'胃癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'809', N'肠癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'810', N'窦性心动过速', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'811', N'窦性心动过缓', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'812', N'房性早搏', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'813', N'心房颤动', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'814', N'心房扑动', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'815', N'阵发性室上性心动过速', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'816', N'室性早搏', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'817', N'室性心动过速', N'486', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'818', N'一度', N'487', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'819', N'二度', N'487', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'820', N'三度', N'487', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'821', N'二度I型', N'487', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'822', N'二度II型', N'487', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'823', N'预激综合征', N'487', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'824', N'未检', N'254', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'825', N'未检', N'266', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'826', N'未检', N'267', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'827', N'未检', N'268', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'828', N'未查', N'261', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'829', N'前列腺癌', N'484', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'830', N'广东', N'96', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'832', N'无', N'527', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'833', N'有', N'527', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'834', N'无', N'528', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'840', N'自理', N'585', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'841', N'依赖', N'585', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'842', N'协助', N'585', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'843', N'自理', N'586', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'844', N'依赖', N'586', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'845', N'协助', N'586', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'847', N'自理', N'587', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'848', N'协助', N'587', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'849', N'依赖', N'587', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'850', N'自理', N'588', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'851', N'协助', N'588', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'852', N'依赖', N'588', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'853', N'普食', N'589', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'854', N'半流', N'589', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'855', N'全流', N'589', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'856', N'特殊饮食', N'589', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'857', N'正常', N'590', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'858', N'增加', N'590', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'859', N'减退', N'590', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'860', N'不思饮食', N'590', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'861', N'正常', N'591', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'894', N'--', N'584', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'896', N'未查', N'263', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'897', N'未查', N'264', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'898', N'未查', N'265', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'903', N'左', N'508', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'904', N'右', N'508', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'905', N'左', N'509', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'906', N'右', N'509', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'907', N'：原因待查。', N'482', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'831', N'无业', N'105', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'835', N'有', N'528', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'836', N'极重度', N'532', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'837', N'重度', N'532', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'838', N'中度', N'532', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'839', N'轻度', N'532', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'846', N'', N'486', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'862', N'腹胀', N'591', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'863', N'便秘', N'591', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'864', N'正常', N'593', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'865', N'尿潴留', N'593', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'866', N'失禁', N'593', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'867', N'尿频', N'593', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'868', N'尿急', N'593', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'869', N'尿痛', N'593', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'872', N'失眠', N'594', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'871', N'正常', N'594', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'873', N'无', N'596', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'874', N'有', N'596', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'875', N'无', N'597', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'876', N'有', N'597', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'878', N'未婚', N'576', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'880', N'已婚', N'576', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'881', N'丧偶', N'576', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'882', N'离异', N'576', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'883', N'--', N'577', N'1')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'884', N'一般', N'577', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'885', N'差', N'577', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'886', N'和睦', N'578', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'887', N'欠佳', N'578', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'888', N'和睦', N'584', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'889', N'欠佳', N'584', N'0')
GO

INSERT INTO [dbo].[MRProjectReference] ([MRReferenceId], [MRReferenceName], [HProjectId], [IsFirst]) VALUES (N'892', N'--', N'578', N'1')
GO

SET IDENTITY_INSERT [dbo].[MRProjectReference] OFF
GO


-- ----------------------------
-- Primary Key structure for table MRProjectReference
-- ----------------------------
ALTER TABLE [dbo].[MRProjectReference] ADD CONSTRAINT [PK_MRProjectReference] PRIMARY KEY NONCLUSTERED ([MRReferenceId])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

