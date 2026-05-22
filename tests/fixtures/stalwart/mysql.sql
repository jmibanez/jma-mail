-- MySQL dump 10.13  Distrib 8.4.9, for Linux (aarch64)
--
-- Host: localhost    Database: stalwart
-- ------------------------------------------------------
-- Server version	8.4.9

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `a`
--

DROP TABLE IF EXISTS `a`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `a` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `a`
--

LOCK TABLES `a` WRITE;
/*!40000 ALTER TABLE `a` DISABLE KEYS */;
/*!40000 ALTER TABLE `a` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `b`
--

DROP TABLE IF EXISTS `b`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `b` (
  `k` blob NOT NULL,
  PRIMARY KEY (`k`(400))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `b`
--

LOCK TABLES `b` WRITE;
/*!40000 ALTER TABLE `b` DISABLE KEYS */;
INSERT INTO `b` VALUES (_binary '\0\0\0\0\0\0\0\0\0\0\0\0\0'),(_binary '\0\0\0admin\0\0\0\0\0\0\0'),(_binary '\0\0\0administrator\0\0\0\0\0\0\0'),(_binary '\0\0\0system\0\0\0\0\0\0\0'),(_binary '\0\0\0admin\0\0\0\0\0\0\0'),(_binary '\0\0\0\İ\0\0\0\0\0\0\0\0\0\0\0\0\0\0'),(_binary '\0%\0example\0\0\0\0\0\0\0'),(_binary '\0%\0org\0\0\0\0\0\0\0'),(_binary '\0S\0administrator\0\0\0\0\0\0\0'),(_binary '\0S\0administrator\0\0\0\0\0\0\0'),(_binary '\0S\0group\0\0\0\0\0\0\0'),(_binary '\0S\0system\0\0\0\0\0\0\0'),(_binary '\0S\0tenant\0\0\0\0\0\0\0'),(_binary '\0S\0user\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0\0\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0Hx—	\0\0'),(_binary 'ÿÿ\0%\0\0\0\0\0\0\0'),(_binary 'ÿÿ\09Hx—\r`'),(_binary 'ÿÿ\0:\0\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0:\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0:\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0>Hx—	\à'),(_binary 'ÿÿ\0>Hx—\n '),(_binary 'ÿÿ\0CHx—\r '),(_binary 'ÿÿ\0CHx—\r@'),(_binary 'ÿÿ\0KHx—À'),(_binary 'ÿÿ\0KHx—\à\n'),(_binary 'ÿÿ\0L\0\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0L\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0L\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0L\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0MHx—@'),(_binary 'ÿÿ\0MHx—`'),(_binary 'ÿÿ\0MHx—€'),(_binary 'ÿÿ\0MHx— '),(_binary 'ÿÿ\0MHx—À\Z'),(_binary 'ÿÿ\0MHx—\0'),(_binary 'ÿÿ\0MHx— '),(_binary 'ÿÿ\0MHx—’@ \0'),(_binary 'ÿÿ\0S\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0S\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0S\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0S\0\0\0\0\0\0\0'),(_binary 'ÿÿ\0^Hx—Ú€¦\0'),(_binary 'ÿÿ\0^Hx—\ÚÀ¨\0'),(_binary 'ÿÿ\0^Hx—\Ú\àª\0'),(_binary 'ÿÿ\0^Hx—\Û ¬\0'),(_binary 'ÿÿ\0^Hx—\Û@®\0'),(_binary 'ÿÿ\0^Hx—\Û`°\0'),(_binary 'ÿÿ\0^Hx—Û€²\0'),(_binary 'ÿÿ\0^Hx—\ÛÀ´\0'),(_binary 'ÿÿ\0^Hx—\Û\à¶\0'),(_binary 'ÿÿ\0^Hx—\Ü ¸\0'),(_binary 'ÿÿ\0^Hx—\Ü@º\0'),(_binary 'ÿÿ\0^Hx—Ü€¼\0'),(_binary 'ÿÿ\0^Hx—Ü ¾\0'),(_binary 'ÿÿ\0^Hx—\ÜÀÀ\0'),(_binary 'ÿÿ\0^Hx—\İ\0\Â\0'),(_binary 'ÿÿ\0^Hx—\İ@\Ä\0'),(_binary 'ÿÿ\0^Hx—\İ`\Æ\0'),(_binary 'ÿÿ\0^Hx—İ€\È\0'),(_binary 'ÿÿ\0cHx—Î€\"\0'),(_binary 'ÿÿ\0cHx—\Î\à$\0'),(_binary 'ÿÿ\0cHx—\Ï &\0'),(_binary 'ÿÿ\0cHx—\Ï`(\0'),(_binary 'ÿÿ\0cHx—Ï *\0'),(_binary 'ÿÿ\0cHx—\ÏÀ,\0'),(_binary 'ÿÿ\0cHx—\Ï\à.\0'),(_binary 'ÿÿ\0cHx—\Ğ 0\0'),(_binary 'ÿÿ\0cHx—\Ğ@2\0'),(_binary 'ÿÿ\0cHx—\Ğ`4\0'),(_binary 'ÿÿ\0cHx—Ğ 6\0'),(_binary 'ÿÿ\0cHx—\Ğ\à8\0'),(_binary 'ÿÿ\0cHx—\Ñ\0:\0'),(_binary 'ÿÿ\0cHx—\Ñ <\0'),(_binary 'ÿÿ\0cHx—\Ñ`>\0'),(_binary 'ÿÿ\0cHx—Ñ€@\0'),(_binary 'ÿÿ\0cHx—Ñ B\0'),(_binary 'ÿÿ\0cHx—\Ñ\àD\0'),(_binary 'ÿÿ\0cHx—\Ò F\0'),(_binary 'ÿÿ\0cHx—\Ò@H\0'),(_binary 'ÿÿ\0cHx—Ò€J\0'),(_binary 'ÿÿ\0cHx—Ò L\0'),(_binary 'ÿÿ\0cHx—\ÒÀN\0'),(_binary 'ÿÿ\0cHx—\Ò\àP\0'),(_binary 'ÿÿ\0cHx—\Ó R\0'),(_binary 'ÿÿ\0cHx—\Ó`T\0'),(_binary 'ÿÿ\0cHx—Ó€V\0'),(_binary 'ÿÿ\0cHx—\ÓÀX\0'),(_binary 'ÿÿ\0cHx—\Ó\àZ\0'),(_binary 'ÿÿ\0cHx—\Ô\0\\\0'),(_binary 'ÿÿ\0cHx—\Ô ^\0'),(_binary 'ÿÿ\0cHx—\Ô@`\0'),(_binary 'ÿÿ\0cHx—\Ô`b\0'),(_binary 'ÿÿ\0cHx—\ÔÀd\0'),(_binary 'ÿÿ\0cHx—\Ô\àf\0'),(_binary 'ÿÿ\0cHx—\Õ\0h\0'),(_binary 'ÿÿ\0cHx—\Õ j\0'),(_binary 'ÿÿ\0cHx—\Õ@l\0'),(_binary 'ÿÿ\0cHx—\Õ`n\0'),(_binary 'ÿÿ\0cHx—Õ p\0'),(_binary 'ÿÿ\0cHx—\ÕÀr\0'),(_binary 'ÿÿ\0cHx—\Ö\0t\0'),(_binary 'ÿÿ\0cHx—\Ö@v\0'),(_binary 'ÿÿ\0cHx—\Ö`x\0'),(_binary 'ÿÿ\0cHx—Ö€z\0'),(_binary 'ÿÿ\0cHx—Ö |\0'),(_binary 'ÿÿ\0cHx—\ÖÀ~\0'),(_binary 'ÿÿ\0cHx—\× €\0'),(_binary 'ÿÿ\0cHx—\×@‚\0'),(_binary 'ÿÿ\0cHx—\×`„\0'),(_binary 'ÿÿ\0cHx—×€†\0'),(_binary 'ÿÿ\0cHx—\×Àˆ\0'),(_binary 'ÿÿ\0cHx—\×\àŠ\0'),(_binary 'ÿÿ\0cHx—\Ø\0Œ\0'),(_binary 'ÿÿ\0cHx—\Ø@\0'),(_binary 'ÿÿ\0cHx—Ø€\0'),(_binary 'ÿÿ\0cHx—Ø ’\0'),(_binary 'ÿÿ\0cHx—\ØÀ”\0'),(_binary 'ÿÿ\0cHx—\Ù\0–\0'),(_binary 'ÿÿ\0cHx—\Ù ˜\0'),(_binary 'ÿÿ\0cHx—Ù€š\0'),(_binary 'ÿÿ\0cHx—Ù œ\0'),(_binary 'ÿÿ\0cHx—\ÙÀ\0'),(_binary 'ÿÿ\0cHx—\Ù\à \0'),(_binary 'ÿÿ\0cHx—\Ú\0¢\0'),(_binary 'ÿÿ\0cHx—\Ú@¤\0'),(_binary 'ÿÿ\0eHx—İ \Ê\0'),(_binary 'ÿÿ\0eHx—\İ\à\Ì\0'),(_binary 'ÿÿ\0eHx—\Ş\0\Î\0'),(_binary 'ÿÿ\0eHx—\Ş \Ğ\0'),(_binary 'ÿÿ\0eHx—\Ş`\Ò\0'),(_binary 'ÿÿ\0eHx—Ş \Ô\0'),(_binary 'ÿÿ\0eHx—\ŞÀ\Ö\0'),(_binary 'ÿÿ\0eHx—\Ş\à\Ø\0'),(_binary 'ÿÿ\0eHx—\ß\0\Ú\0'),(_binary 'ÿÿ\0eHx—\ß \Ü\0'),(_binary 'ÿÿ\0eHx—\ß@\Ş\0'),(_binary 'ÿÿ\0eHx—\ß`\à\0'),(_binary 'ÿÿ\0eHx—ß \â\0'),(_binary 'ÿÿ\0eHx—\ßÀ\ä\0'),(_binary 'ÿÿ\0eHx—\ß\à\æ\0'),(_binary 'ÿÿ\0eHx—\à \è\0'),(_binary 'ÿÿ\0eHx—\à@\ê\0'),(_binary 'ÿÿ\0eHx—\à€\ì\0'),(_binary 'ÿÿ\0eHx—\à \î\0'),(_binary 'ÿÿ\0eHx—\àÀ\ğ\0'),(_binary 'ÿÿ\0eHx—\á\0\ò\0'),(_binary 'ÿÿ\0eHx—\á@\ô\0'),(_binary 'ÿÿ\0eHx—\á`\ö\0'),(_binary 'ÿÿ\0eHx—\á€ø\0'),(_binary 'ÿÿ\0eHx—\á ú\0'),(_binary 'ÿÿ\0eHx—\áÀü\0'),(_binary 'ÿÿ\0eHx—\â\0ş\0'),(_binary 'ÿÿ\0eHx—\âA\0\0'),(_binary 'ÿÿ\0eHx—\âa\0'),(_binary 'ÿÿ\0eHx—\â\0'),(_binary 'ÿÿ\0eHx—\âÁ\0'),(_binary 'ÿÿ\0eHx—\â\á\0'),(_binary 'ÿÿ\0eHx—\ã\n\0'),(_binary 'ÿÿ\0eHx—\ã!\0'),(_binary 'ÿÿ\0eHx—\ãa\0'),(_binary 'ÿÿ\0eHx—\ã\0'),(_binary 'ÿÿ\0eHx—\ãÁ\0'),(_binary 'ÿÿ\0eHx—\ã\á\0'),(_binary 'ÿÿ\0eHx—\ä\0'),(_binary 'ÿÿ\0eHx—\êA\0'),(_binary 'ÿÿ\0eHx—\ê\Z\0'),(_binary 'ÿÿ\0eHx—\ê¡\0'),(_binary 'ÿÿ\0eHx—\êÁ\0'),(_binary 'ÿÿ\0eHx—\ê\á \0'),(_binary 'ÿÿ\0eHx—\ë!\"\0'),(_binary 'ÿÿ\0eHx—\ëA$\0'),(_binary 'ÿÿ\0eHx—\ëa&\0'),(_binary 'ÿÿ\0eHx—\ë(\0'),(_binary 'ÿÿ\0eHx—\ë¡*\0'),(_binary 'ÿÿ\0eHx—\ëÁ,\0'),(_binary 'ÿÿ\0eHx—\ë\á.\0'),(_binary 'ÿÿ\0eHx—\ì0\0'),(_binary 'ÿÿ\0eHx—\ìA2\0'),(_binary 'ÿÿ\0eHx—\ìa4\0'),(_binary 'ÿÿ\0eHx—\ì6\0'),(_binary 'ÿÿ\0eHx—\ì¡8\0'),(_binary 'ÿÿ\0eHx—\ìÁ:\0'),(_binary 'ÿÿ\0eHx—\ìÁ<\0'),(_binary 'ÿÿ\0eHx—\ì\á>\0'),(_binary 'ÿÿ\0eHx—\í!@\0'),(_binary 'ÿÿ\0eHx—\íAB\0'),(_binary 'ÿÿ\0eHx—\íaD\0'),(_binary 'ÿÿ\0eHx—\í¡F\0'),(_binary 'ÿÿ\0eHx—\íÁH\0'),(_binary 'ÿÿ\0eHx—\í\áJ\0'),(_binary 'ÿÿ\0eHx—\í\áL\0'),(_binary 'ÿÿ\0eHx—\îAN\0'),(_binary 'ÿÿ\0eHx—\îaP\0'),(_binary 'ÿÿ\0eHx—\îR\0'),(_binary 'ÿÿ\0eHx—\î¡T\0'),(_binary 'ÿÿ\0eHx—\î¡V\0'),(_binary 'ÿÿ\0eHx—\îÁX\0'),(_binary 'ÿÿ\0eHx—\î\áZ\0'),(_binary 'ÿÿ\0eHx—\ïA\\\0'),(_binary 'ÿÿ\0eHx—\ïa^\0'),(_binary 'ÿÿ\0eHx—\ï¡`\0'),(_binary 'ÿÿ\0eHx—\ïÁb\0'),(_binary 'ÿÿ\0eHx—\ğd\0'),(_binary 'ÿÿ\0eHx—\ğ!f\0'),(_binary 'ÿÿ\0eHx—\ğAh\0'),(_binary 'ÿÿ\0eHx—\ğaj\0'),(_binary 'ÿÿ\0eHx—\ğl\0'),(_binary 'ÿÿ\0eHx—\ğ¡n\0'),(_binary 'ÿÿ\0eHx—\ğÁp\0'),(_binary 'ÿÿ\0eHx—\ğ\ár\0'),(_binary 'ÿÿ\0eHx—\ñt\0'),(_binary 'ÿÿ\0eHx—\ñ!v\0'),(_binary 'ÿÿ\0eHx—\ñ!x\0'),(_binary 'ÿÿ\0eHx—\ñAz\0'),(_binary 'ÿÿ\0eHx—\ñ|\0'),(_binary 'ÿÿ\0eHx—\ñÁ~\0'),(_binary 'ÿÿ\0eHx—\ñ\á€\0'),(_binary 'ÿÿ\0eHx—\ò‚\0'),(_binary 'ÿÿ\0eHx—\ò!„\0'),(_binary 'ÿÿ\0eHx—\òA†\0'),(_binary 'ÿÿ\0eHx—\òaˆ\0'),(_binary 'ÿÿ\0eHx—\òŠ\0'),(_binary 'ÿÿ\0eHx—\òÁŒ\0'),(_binary 'ÿÿ\0eHx—\ò\á\0'),(_binary 'ÿÿ\0eHx—\ó\0'),(_binary 'ÿÿ\0eHx—\óA’\0'),(_binary 'ÿÿ\0eHx—\óa”\0'),(_binary 'ÿÿ\0eHx—\ó–\0'),(_binary 'ÿÿ\0eHx—\ó¡˜\0'),(_binary 'ÿÿ\0eHx—\óÁš\0'),(_binary 'ÿÿ\0eHx—\ó\áœ\0'),(_binary 'ÿÿ\0eHx—\ô\0'),(_binary 'ÿÿ\0eHx—\ôa \0'),(_binary 'ÿÿ\0eHx—\ô¢\0'),(_binary 'ÿÿ\0eHx—\ô¡¤\0'),(_binary 'ÿÿ\0eHx—\ôÁ¦\0'),(_binary 'ÿÿ\0eHx—\õ¨\0'),(_binary 'ÿÿ\0eHx—\õ!ª\0'),(_binary 'ÿÿ\0eHx—\õA¬\0'),(_binary 'ÿÿ\0eHx—\õa®\0'),(_binary 'ÿÿ\0eHx—\õ°\0'),(_binary 'ÿÿ\0eHx—\õ¡²\0'),(_binary 'ÿÿ\0eHx—\õ\á´\0'),(_binary 'ÿÿ\0eHx—\ö¶\0'),(_binary 'ÿÿ\0eHx—\ö!¸\0'),(_binary 'ÿÿ\0eHx—\öAº\0'),(_binary 'ÿÿ\0eHx—\öa¼\0'),(_binary 'ÿÿ\0eHx—\ö¡¾\0'),(_binary 'ÿÿ\0eHx—\öÁÀ\0'),(_binary 'ÿÿ\0eHx—\ö\á\Â\0'),(_binary 'ÿÿ\0eHx—\÷A\Ä\0'),(_binary 'ÿÿ\0eHx—\÷a\Æ\0'),(_binary 'ÿÿ\0eHx—\÷\È\0'),(_binary 'ÿÿ\0eHx—\÷¡\Ê\0'),(_binary 'ÿÿ\0eHx—\÷¡\Ì\0'),(_binary 'ÿÿ\0eHx—\÷Á\Î\0'),(_binary 'ÿÿ\0eHx—\÷\á\Ğ\0'),(_binary 'ÿÿ\0eHx—ø\Ò\0'),(_binary 'ÿÿ\0eHx—ø!\Ô\0'),(_binary 'ÿÿ\0eHx—øa\Ö\0'),(_binary 'ÿÿ\0eHx—ø\Ø\0'),(_binary 'ÿÿ\0eHx—ø¡\Ú\0'),(_binary 'ÿÿ\0eHx—øÁ\Ü\0'),(_binary 'ÿÿ\0eHx—ø\á\Ş\0'),(_binary 'ÿÿ\0eHx—ù\à\0'),(_binary 'ÿÿ\0eHx—ù!\â\0'),(_binary 'ÿÿ\0eHx—ùa\ä\0'),(_binary 'ÿÿ\0eHx—ùa\æ\0'),(_binary 'ÿÿ\0eHx—ù\è\0'),(_binary 'ÿÿ\0eHx—ù¡\ê\0'),(_binary 'ÿÿ\0eHx—ùÁ\ì\0'),(_binary 'ÿÿ\0eHx—ù\á\î\0'),(_binary 'ÿÿ\0eHx—ú\ğ\0'),(_binary 'ÿÿ\0eHx—úA\ò\0'),(_binary 'ÿÿ\0eHx—ú\ô\0'),(_binary 'ÿÿ\0eHx—ú¡\ö\0'),(_binary 'ÿÿ\0eHx—ú\áø\0'),(_binary 'ÿÿ\0eHx—û!ú\0'),(_binary 'ÿÿ\0eHx—ûAü\0'),(_binary 'ÿÿ\0eHx—ûaş\0'),(_binary 'ÿÿ\0eHx—û\Â\0\0'),(_binary 'ÿÿ\0eHx—û\â\0'),(_binary 'ÿÿ\0eHx—û\â\0'),(_binary 'ÿÿ\0eHx—ü\0'),(_binary 'ÿÿ\0eHx—ü\"\0'),(_binary 'ÿÿ\0eHx—üB\n\0'),(_binary 'ÿÿ\0eHx—üb\0'),(_binary 'ÿÿ\0eHx—ü‚\0'),(_binary 'ÿÿ\0eHx—ü\â\0'),(_binary 'ÿÿ\0eHx—ü\â\0'),(_binary 'ÿÿ\0eHx—ı\0'),(_binary 'ÿÿ\0eHx—ı\"\0'),(_binary 'ÿÿ\0eHx—ıB\0'),(_binary 'ÿÿ\0eHx—ıb\Z\0'),(_binary 'ÿÿ\0eHx—ı‚\0'),(_binary 'ÿÿ\0eHx—ı¢\0'),(_binary 'ÿÿ\0eHx—ı\Â \0'),(_binary 'ÿÿ\0eHx—ı\â\"\0'),(_binary 'ÿÿ\0eHx—ş$\0'),(_binary 'ÿÿ\0eHx—şB&\0'),(_binary 'ÿÿ\0eHx—şb(\0'),(_binary 'ÿÿ\0eHx—ş‚*\0'),(_binary 'ÿÿ\0eHx—ş¢,\0'),(_binary 'ÿÿ\0eHx—ş\Â.\0'),(_binary 'ÿÿ\0eHx—ş\â0\0'),(_binary 'ÿÿ\0eHx—ÿ2\0'),(_binary 'ÿÿ\0eHx—ÿ\"4\0'),(_binary 'ÿÿ\0eHx—ÿb6\0'),(_binary 'ÿÿ\0eHx—ÿ‚8\0'),(_binary 'ÿÿ\0eHx—ÿ¢:\0'),(_binary 'ÿÿ\0eHx—ÿ\Â<\0'),(_binary 'ÿÿ\0eHx—ÿ\â>\0'),(_binary 'ÿÿ\0eHx˜\0@\0'),(_binary 'ÿÿ\0eHx˜\0\"B\0'),(_binary 'ÿÿ\0eHx˜\0BD\0'),(_binary 'ÿÿ\0eHx˜\0‚F\0'),(_binary 'ÿÿ\0eHx˜\0¢H\0'),(_binary 'ÿÿ\0eHx˜\0\ÂJ\0'),(_binary 'ÿÿ\0eHx˜\0\âL\0'),(_binary 'ÿÿ\0eHx˜N\0'),(_binary 'ÿÿ\0eHx˜\"P\0'),(_binary 'ÿÿ\0eHx˜BR\0'),(_binary 'ÿÿ\0eHx˜bT\0'),(_binary 'ÿÿ\0eHx˜¢V\0'),(_binary 'ÿÿ\0eHx˜\ÂX\0'),(_binary 'ÿÿ\0eHx˜\âZ\0'),(_binary 'ÿÿ\0eHx˜\\\0'),(_binary 'ÿÿ\0eHx˜\"^\0'),(_binary 'ÿÿ\0eHx˜\"`\0'),(_binary 'ÿÿ\0eHx˜Bb\0'),(_binary 'ÿÿ\0eHx˜bd\0'),(_binary 'ÿÿ\0eHx˜¢f\0'),(_binary 'ÿÿ\0eHx˜¢h\0'),(_binary 'ÿÿ\0eHx˜\âj\0'),(_binary 'ÿÿ\0eHx˜\"l\0'),(_binary 'ÿÿ\0eHx˜bn\0'),(_binary 'ÿÿ\0eHx˜‚p\0'),(_binary 'ÿÿ\0eHx˜¢r\0'),(_binary 'ÿÿ\0eHx˜\Ât\0'),(_binary 'ÿÿ\0eHx˜v\0'),(_binary 'ÿÿ\0eHx˜\"x\0'),(_binary 'ÿÿ\0eHx˜Bz\0'),(_binary 'ÿÿ\0eHx˜b|\0'),(_binary 'ÿÿ\0eHx˜‚~\0'),(_binary 'ÿÿ\0eHx˜¢€\0'),(_binary 'ÿÿ\0eHx˜Â‚\0'),(_binary 'ÿÿ\0eHx˜„\0'),(_binary 'ÿÿ\0eHx˜\"†\0'),(_binary 'ÿÿ\0eHx˜Bˆ\0'),(_binary 'ÿÿ\0eHx˜bŠ\0'),(_binary 'ÿÿ\0eHx˜‚Œ\0'),(_binary 'ÿÿ\0eHx˜¢\0'),(_binary 'ÿÿ\0eHx˜Â\0'),(_binary 'ÿÿ\0eHx˜\â’\0'),(_binary 'ÿÿ\0eHx˜”\0'),(_binary 'ÿÿ\0eHx˜B–\0'),(_binary 'ÿÿ\0eHx˜b˜\0'),(_binary 'ÿÿ\0eHx˜‚š\0'),(_binary 'ÿÿ\0eHx˜¢œ\0'),(_binary 'ÿÿ\0eHx˜Â\0'),(_binary 'ÿÿ\0eHx˜\â \0'),(_binary 'ÿÿ\0eHx˜\"¢\0'),(_binary 'ÿÿ\0eHx˜B¤\0'),(_binary 'ÿÿ\0eHx˜b¦\0'),(_binary 'ÿÿ\0eHx˜‚¨\0'),(_binary 'ÿÿ\0eHx˜¢ª\0'),(_binary 'ÿÿ\0eHx˜Â¬\0'),(_binary 'ÿÿ\0eHx˜\â®\0'),(_binary 'ÿÿ\0eHx˜°\0'),(_binary 'ÿÿ\0eHx˜\"²\0'),(_binary 'ÿÿ\0eHx˜B´\0'),(_binary 'ÿÿ\0eHx˜‚¶\0'),(_binary 'ÿÿ\0eHx˜‚¸\0'),(_binary 'ÿÿ\0eHx˜¢º\0'),(_binary 'ÿÿ\0eHx˜Â¼\0'),(_binary 'ÿÿ\0eHx˜\â¾\0'),(_binary 'ÿÿ\0eHx˜	À\0'),(_binary 'ÿÿ\0eHx˜	\"\Â\0'),(_binary 'ÿÿ\0eHx˜	b\Ä\0'),(_binary 'ÿÿ\0eHx˜	‚\Æ\0'),(_binary 'ÿÿ\0eHx˜	¢\È\0'),(_binary 'ÿÿ\0eHx˜	\Â\Ê\0'),(_binary 'ÿÿ\0eHx˜	\â\Ì\0'),(_binary 'ÿÿ\0eHx˜\n\Î\0'),(_binary 'ÿÿ\0eHx˜\n\"\Ğ\0'),(_binary 'ÿÿ\0eHx˜\nB\Ò\0'),(_binary 'ÿÿ\0eHx˜\nb\Ô\0'),(_binary 'ÿÿ\0eHx˜\n‚\Ö\0'),(_binary 'ÿÿ\0eHx˜\n\Â\Ø\0'),(_binary 'ÿÿ\0eHx˜\n\â\Ú\0'),(_binary 'ÿÿ\0eHx˜\Ü\0'),(_binary 'ÿÿ\0eHx˜\"\Ş\0'),(_binary 'ÿÿ\0eHx˜B\à\0'),(_binary 'ÿÿ\0eHx˜b\â\0'),(_binary 'ÿÿ\0eHx˜‚\ä\0'),(_binary 'ÿÿ\0eHx˜\Â\æ\0'),(_binary 'ÿÿ\0eHx˜\â\è\0'),(_binary 'ÿÿ\0eHx˜\ê\0'),(_binary 'ÿÿ\0eHx˜\"\ì\0'),(_binary 'ÿÿ\0eHx˜B\î\0'),(_binary 'ÿÿ\0eHx˜‚\ğ\0'),(_binary 'ÿÿ\0eHx˜\Â\ò\0'),(_binary 'ÿÿ\0eHx˜\Â\ô\0'),(_binary 'ÿÿ\0eHx˜\â\ö\0'),(_binary 'ÿÿ\0eHx˜\r\"ø\0'),(_binary 'ÿÿ\0eHx˜\rBú\0'),(_binary 'ÿÿ\0eHx˜\r‚ü\0'),(_binary 'ÿÿ\0eHx˜\r¢ş\0'),(_binary 'ÿÿ\0eHx˜\r\Ã\0\0'),(_binary 'ÿÿ\0eHx˜\r\Ã\0'),(_binary 'ÿÿ\0eHx˜#\0'),(_binary 'ÿÿ\0eHx˜C\0'),(_binary 'ÿÿ\0eHx˜c\0'),(_binary 'ÿÿ\0eHx˜c\n\0'),(_binary 'ÿÿ\0eHx˜ƒ\0'),(_binary 'ÿÿ\0eHx˜£\0'),(_binary 'ÿÿ\0eHx˜\Ã\0'),(_binary 'ÿÿ\0eHx˜#\0'),(_binary 'ÿÿ\0eHx˜C\0'),(_binary 'ÿÿ\0eHx˜ƒ\0'),(_binary 'ÿÿ\0eHx˜ƒ\0'),(_binary 'ÿÿ\0eHx˜£\Z\0'),(_binary 'ÿÿ\0eHx˜\Ã\0'),(_binary 'ÿÿ\0eHx˜\0'),(_binary 'ÿÿ\0eHx˜# \0'),(_binary 'ÿÿ\0eHx˜C\"\0'),(_binary 'ÿÿ\0eHx˜c$\0'),(_binary 'ÿÿ\0eHx˜ƒ&\0'),(_binary 'ÿÿ\0eHx˜\Ã(\0'),(_binary 'ÿÿ\0eHx˜\ã*\0'),(_binary 'ÿÿ\0eHx˜,\0'),(_binary 'ÿÿ\0eHx˜#.\0'),(_binary 'ÿÿ\0eHx˜C0\0'),(_binary 'ÿÿ\0eHx˜ƒ2\0'),(_binary 'ÿÿ\0eHx˜£4\0'),(_binary 'ÿÿ\0eHx˜\Ã6\0'),(_binary 'ÿÿ\0eHx˜\ã8\0'),(_binary 'ÿÿ\0eHx˜:\0'),(_binary 'ÿÿ\0eHx˜C<\0'),(_binary 'ÿÿ\0eHx˜c>\0'),(_binary 'ÿÿ\0eHx˜ƒ@\0'),(_binary 'ÿÿ\0eHx˜£B\0'),(_binary 'ÿÿ\0eHx˜\ãD\0'),(_binary 'ÿÿ\0eHx˜\ãF\0'),(_binary 'ÿÿ\0eHx˜H\0'),(_binary 'ÿÿ\0eHx˜#J\0'),(_binary 'ÿÿ\0eHx˜CL\0'),(_binary 'ÿÿ\0eHx˜£N\0'),(_binary 'ÿÿ\0eHx˜\ÃP\0'),(_binary 'ÿÿ\0eHx˜\ÃR\0'),(_binary 'ÿÿ\0eHx˜\ãT\0'),(_binary 'ÿÿ\0eHx˜V\0'),(_binary 'ÿÿ\0eHx˜#X\0'),(_binary 'ÿÿ\0eHx˜CZ\0'),(_binary 'ÿÿ\0eHx˜£\\\0'),(_binary 'ÿÿ\0eHx˜\ã^\0'),(_binary 'ÿÿ\0eHx˜`\0'),(_binary 'ÿÿ\0eHx˜#b\0'),(_binary 'ÿÿ\0eHx˜Cd\0'),(_binary 'ÿÿ\0eHx˜cf\0'),(_binary 'ÿÿ\0eHx˜ch\0'),(_binary 'ÿÿ\0eHx˜£j\0'),(_binary 'ÿÿ\0eHx˜\Ãl\0'),(_binary 'ÿÿ\0eHx˜n\0'),(_binary 'ÿÿ\0qHx’V€\0\0');
/*!40000 ALTER TABLE `b` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `d`
--

DROP TABLE IF EXISTS `d`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `d` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `d`
--

LOCK TABLES `d` WRITE;
/*!40000 ALTER TABLE `d` DISABLE KEYS */;
INSERT INTO `d` VALUES (_binary '\0\0\0\0\0\0\0\0\0',_binary '\0\0admin\0\0a$argon2id$v=19$m=19456,t=2,p=1$Hz6ETxdaTPJJkhWy6kDfTQ$ru0LifDXamf/3BXIuuDP6p3E1AB8eFkvsYk9ejdkncs\0\0\0jma-fixture-seedera$argon2id$v=19$m=19456,t=2,p=1$toNdYf69TwbY5myCtMaxgg$NSOcr1jUQXZyDsnE5ObShoE2KdVhDBP6m3wO+v4s0xg\0\0\0\0j0^\0\0\0\0\0\0\0j0T\0\0\0\0\0System administratorg\0\0'),(_binary '\0%\0\0\0\0\0\0\0',_binary '\0example.org\0\0\0\0\0j0T\0\0\0\0\0\0\0\0\0mailto:postmaster'),(_binary '\0S\0\0\0\0\0\0\0',_binary '\0User\0\0\ô\0	\n\r\Z !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹Œ‘’“”•–—˜™š›œŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁ\Â\Ã\Ä\Å\Æ\Ç\È\É\Ê\Ë\Ì\Í\Î\Ï\Ğ\Ñ\Ò\Ó\Ô\Õ\à\á\â\ãŠ‹Œ‘’“™š›œ—˜™š›‚ƒ„…\È\Ê\Ë\Ì“\0'),(_binary '\0S\0\0\0\0\0\0\0',_binary '\0Group\0\0\å	\n\r\Z !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹Œ‘’“”•–—˜™š›œŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁ\Â\Ã\Ä\Å\Æ\Ç\È\É\Ê\Ë\Ì\Í\Î\Ï\Ğ\Ñ\Ò\Ó\Ô\Õ\â\ã™š›œ—˜™š›‚ƒ„…\È\Ê\Ë\Ì“\0'),(_binary '\0S\0\0\0\0\0\0\0',_binary '\0Tenant Administrator\0\02\0\Ú\Û\Ü\İ\Ş\ß\ä\å\æ\ç\è\Ô\Õ\Ö\×\Ø\ç\è\é\ê\ë\ì\í\î\ï\ğ’“”•–úûüış†‡ˆ‰Š‘\0'),(_binary '\0S\0\0\0\0\0\0\0',_binary '\0System Administrator\0\0\Ä\0\Ö\×\Ø\Ù\Ú\Û\Ü\İ\Ş\ß\à\á\â\ã\ä\å\æ\ç\è\é\ê\ë\ì\í\î\ï\ğ\ñ\ò\ó\ô\õ\ö\÷øùúûüışÿ€‚ƒ„…†‡ˆ‰Š‹Œ‘’“”•–—˜™š›œŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁ\Â\Ã\Ä\Å\Æ\Ç\È\É\Ê\Ë\Ì\Í\Î\Ï\Ğ\Ñ\Ò\Ó\Ô\Õ\Ö\×\Ø\Ù\Ú\Û\Ü\İ\Ş\ß\à\á\â\ã\ä\å\æ\ç\è\é\ê\ë\ì\í\î\ï\ğ\ñ\ò\ó\ô\õ\ö\÷øùúûüışÿ€‚ƒ„…†‡ˆ‰Š‹Œ‘’“”•–—˜™š›œŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁ\Â\Ã\Ä\Å\Æ\Ç\È\É\Ê\Ë\Ì\Í\Î\Ï\Ğ\Ñ\Ò\Ó\Ô\Õ\Ö\×\Ø\Ù\Ú\Û\Ü\İ\Ş\ß\à\á\â\ã\ä\å\æ\ç\è\é\ê\ë\ì\í\î\ï\ğ\ñ\ò\ó\ô\õ\ö\÷øùúûüışÿ€‚ƒ„…†‡ˆ‰Š‹Œ‘’“”•–—˜™š›œŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁ\Â\Ã\Ä\Å\Æ\Ç\È\É\Ê\Ë\Ì\Í\Î\Ï\Ğ\Ñ\Ò\Ó\Ô\Õ\Ö\×\Ø\Ù\Ú\Û\Ü\İ\Ş\ß\à\á\â\ã\ä\å\æ\ç\è\é\ê\ë\ì\í\î\ï\ğ\ñ\ò\ó\ô\õ\ö\÷øùúûüışÿ€‚ƒ„…†‡ˆ‰Š‹Œ‘’\0');
/*!40000 ALTER TABLE `d` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `e`
--

DROP TABLE IF EXISTS `e`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `e` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `e`
--

LOCK TABLES `e` WRITE;
/*!40000 ALTER TABLE `e` DISABLE KEYS */;
/*!40000 ALTER TABLE `e` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `f`
--

DROP TABLE IF EXISTS `f`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `f` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `f`
--

LOCK TABLES `f` WRITE;
/*!40000 ALTER TABLE `f` DISABLE KEYS */;
INSERT INTO `f` VALUES (_binary '\0\0\0\0\0\0\0\0Hx—À\0',_binary '\0\0\0\0\0\0j0]\0\0\0\0j0]'),(_binary '\0\0\0\0j0]Hx—À\0',_binary '\0');
/*!40000 ALTER TABLE `f` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `g`
--

DROP TABLE IF EXISTS `g`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `g` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `g`
--

LOCK TABLES `g` WRITE;
/*!40000 ALTER TABLE `g` DISABLE KEYS */;
INSERT INTO `g` VALUES (_binary '\0%\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',''),(_binary '\0%\0\0\0\0\0\0\0\0i\0\0CL²M\Í',''),(_binary '\0%\0example.org',_binary '\0%\0\0\0\0\0\0\0'),(_binary '\09\0default',_binary '\09Hx—\r`'),(_binary '\0:\0dsn',_binary '\0:\0\0\0\0\0\0\0'),(_binary '\0:\0local',_binary '\0:\0\0\0\0\0\0\0\0'),(_binary '\0:\0remote',_binary '\0:\0\0\0\0\0\0\0'),(_binary '\0C\0local',_binary '\0CHx—\r@'),(_binary '\0C\0mx',_binary '\0CHx—\r '),(_binary '\0K\0default',_binary '\0KHx—\à\n'),(_binary '\0K\0invalid-tls',_binary '\0KHx—À'),(_binary '\0L\0\0\0\0\0\0\0\0\0:\0\0\0\0\0\0\0\0',''),(_binary '\0L\0\0\0\0\0\0\0\0:\0\0\0\0\0\0\0',''),(_binary '\0L\0\0\0\0\0\0\0\0:\0\0\0\0\0\0\0',''),(_binary '\0L\0dsn',_binary '\0L\0\0\0\0\0\0\0'),(_binary '\0L\0local',_binary '\0L\0\0\0\0\0\0\0\0'),(_binary '\0L\0remote',_binary '\0L\0\0\0\0\0\0\0'),(_binary '\0L\0report',_binary '\0L\0\0\0\0\0\0\0'),(_binary '\0M\0http',_binary '\0MHx— '),(_binary '\0M\0https',_binary '\0MHx—\0'),(_binary '\0M\0imap-plain',_binary '\0MHx—’@ \0'),(_binary '\0M\0imaps',_binary '\0MHx—€'),(_binary '\0M\0pop3s',_binary '\0MHx— '),(_binary '\0M\0sieve',_binary '\0MHx—À\Z'),(_binary '\0M\0smtp',_binary '\0MHx—@'),(_binary '\0M\0submissions',_binary '\0MHx—`'),(_binary '\0S\0\0\0\0\0\0\0\0\0\0CL²M\Í',''),(_binary '\0S\0\0\0\0\0\0\0\0\0\0CL²M\Í',''),(_binary '\0S\0\0\0\0\0\0\0\0\0\0CL²M\Í',''),(_binary '\0S\0\0\0\0\0\0\0\0\0\0CL²M\Í',''),(_binary '\0^\0STWT_DBL_SPAMHAUS_DOMAIN',_binary '\0^Hx—\Ü@º\0'),(_binary '\0^\0STWT_DNSWL_IP',_binary '\0^Hx—\Ü ¸\0'),(_binary '\0^\0STWT_DWL_DNSWL_DOMAIN',_binary '\0^Hx—\İ@\Ä\0'),(_binary '\0^\0STWT_MSBL_EBL_EMAIL',_binary '\0^Hx—\İ`\Æ\0'),(_binary '\0^\0STWT_RBL_BARRACUDA_IP',_binary '\0^Hx—\ÛÀ´\0'),(_binary '\0^\0STWT_RBL_BLOCKLISTDE_IP',_binary '\0^Hx—\Û\à¶\0'),(_binary '\0^\0STWT_RBL_MAILSPIKE_IP',_binary '\0^Hx—\ÚÀ¨\0'),(_binary '\0^\0STWT_RBL_SEM_IP',_binary '\0^Hx—\Û@®\0'),(_binary '\0^\0STWT_RBL_SENDERSCORE_IP',_binary '\0^Hx—\Ú\àª\0'),(_binary '\0^\0STWT_RBL_SENDERSCORE_REPUT_IP',_binary '\0^Hx—\Û ¬\0'),(_binary '\0^\0STWT_RBL_SPAMCOP_IP',_binary '\0^Hx—Û€²\0'),(_binary '\0^\0STWT_RBL_SPAMHAUS_IP',_binary '\0^Hx—Ú€¦\0'),(_binary '\0^\0STWT_RBL_VIRUSFREE_IP',_binary '\0^Hx—\Û`°\0'),(_binary '\0^\0STWT_SEM_URIBL',_binary '\0^Hx—\ÜÀÀ\0'),(_binary '\0^\0STWT_SEM_URIBL_FRESH15',_binary '\0^Hx—\İ\0\Â\0'),(_binary '\0^\0STWT_SURBL_DOMAIN',_binary '\0^Hx—Ü€¼\0'),(_binary '\0^\0STWT_SURBL_HASHBL_DOMAIN',_binary '\0^Hx—İ€\È\0'),(_binary '\0^\0STWT_URIBL_DOMAIN',_binary '\0^Hx—Ü ¾\0'),(_binary '\0c\0STWT_ARC_SIGNED',_binary '\0cHx—Ò L\0'),(_binary '\0c\0STWT_AUTH_NA',_binary '\0cHx—Ï *\0'),(_binary '\0c\0STWT_AUTH_NA_FAIL',_binary '\0cHx—\ÏÀ,\0'),(_binary '\0c\0STWT_AUTOGEN_PHP_SPAMMY',_binary '\0cHx—\Ñ <\0'),(_binary '\0c\0STWT_BLOCKED_DOMAIN',_binary '\0cHx—\Õ`n\0'),(_binary '\0c\0STWT_BOUNCE',_binary '\0cHx—\Õ\0h\0'),(_binary '\0c\0STWT_BOUNCE_NO_AUTH',_binary '\0cHx—\Ï\à.\0'),(_binary '\0c\0STWT_BOUNCE_SUBJECT',_binary '\0cHx—Ø ’\0'),(_binary '\0c\0STWT_COMPROMISED_ACCT_BULK',_binary '\0cHx—\Ğ@2\0'),(_binary '\0c\0STWT_DIRECT_TO_MX',_binary '\0cHx—\Ô\0\\\0'),(_binary '\0c\0STWT_DKIM_SIGNED',_binary '\0cHx—Ò€J\0'),(_binary '\0c\0STWT_DMARC_ALLOW_WITH_FAIL',_binary '\0cHx—\Ï`(\0'),(_binary '\0c\0STWT_FAKE_REPLY',_binary '\0cHx—\Ù ˜\0'),(_binary '\0c\0STWT_FORGED_RCPT_LIST',_binary '\0cHx—\Î\à$\0'),(_binary '\0c\0STWT_FORGED_SENDER_LIST',_binary '\0cHx—\Ï &\0'),(_binary '\0c\0STWT_FREEMAIL_AFF',_binary '\0cHx—\Ñ\àD\0'),(_binary '\0c\0STWT_FREEMAIL_RTO_NEQ_DOM',_binary '\0cHx—\ÒÀN\0'),(_binary '\0c\0STWT_FREE_OR_DISP',_binary '\0cHx—\Ø@\0'),(_binary '\0c\0STWT_FROM_NAME_SPACE',_binary '\0cHx—\Ô\àf\0'),(_binary '\0c\0STWT_HACKED_WP_PHISHING',_binary '\0cHx—\Ğ 0\0'),(_binary '\0c\0STWT_HAS_ANON_DOMAIN',_binary '\0cHx—Ñ€@\0'),(_binary '\0c\0STWT_HAS_GOOGLE_URL',_binary '\0cHx—\ÙÀ\0'),(_binary '\0c\0STWT_HAS_PHPMAILER_SIG',_binary '\0cHx—×€†\0'),(_binary '\0c\0STWT_HAS_SEO_WORD',_binary '\0cHx—\ÓÀX\0'),(_binary '\0c\0STWT_HAS_TITLE',_binary '\0cHx—\ÔÀd\0'),(_binary '\0c\0STWT_HAS_X_AS',_binary '\0cHx—\ÕÀr\0'),(_binary '\0c\0STWT_HAS_X_GMSV',_binary '\0cHx—\Ö\0t\0'),(_binary '\0c\0STWT_HIDDEN_SOURCE_OBJ',_binary '\0cHx—\Ö@v\0'),(_binary '\0c\0STWT_HIDDEN_SOURCE_PHP',_binary '\0cHx—Ö€z\0'),(_binary '\0c\0STWT_INFO_INFO_LU',_binary '\0cHx—\×\àŠ\0'),(_binary '\0c\0STWT_IPFS_OR_ONION',_binary '\0cHx—\Ù\à \0'),(_binary '\0c\0STWT_KLMS_SPAM',_binary '\0cHx—\× €\0'),(_binary '\0c\0STWT_LONG_SUBJ',_binary '\0cHx—\Ù\0–\0'),(_binary '\0c\0STWT_MIME_BAD_EXT_WITH_BAD_UNICODE',_binary '\0cHx—Ó€V\0'),(_binary '\0c\0STWT_PHISHED_OPEN',_binary '\0cHx—Ù€š\0'),(_binary '\0c\0STWT_PHISHED_TANK',_binary '\0cHx—Ù œ\0'),(_binary '\0c\0STWT_PHISH_EMOTION',_binary '\0cHx—\Ñ`>\0'),(_binary '\0c\0STWT_RCPT_DOMAIN_IN_MESSAGE',_binary '\0cHx—\Õ j\0'),(_binary '\0c\0STWT_RCVD_DKIM_ARC_DNSWL_HI',_binary '\0cHx—\Ñ\0:\0'),(_binary '\0c\0STWT_RCVD_DKIM_ARC_DNSWL_MED',_binary '\0cHx—\Ğ\à8\0'),(_binary '\0c\0STWT_RCVD_UNAUTH_PBL',_binary '\0cHx—Ğ 6\0'),(_binary '\0c\0STWT_REDIRECTOR_URL_ONLY',_binary '\0cHx—\Ò F\0'),(_binary '\0c\0STWT_R_UNDISC_RCPT',_binary '\0cHx—Õ p\0'),(_binary '\0c\0STWT_SABUSE_FROM_INJECTOR',_binary '\0cHx—\Ó`T\0'),(_binary '\0c\0STWT_SEO_SPAM',_binary '\0cHx—\Ó\àZ\0'),(_binary '\0c\0STWT_SHORT_BAD_HEADERS',_binary '\0cHx—Î€\"\0'),(_binary '\0c\0STWT_SHORT_LINK_IMG',_binary '\0cHx—\Ô ^\0'),(_binary '\0c\0STWT_SPAM_FLAG',_binary '\0cHx—\ÖÀ~\0'),(_binary '\0c\0STWT_SUBJECT_HAS_SYMBOLS',_binary '\0cHx—\ØÀ”\0'),(_binary '\0c\0STWT_SUBJ_BOUNCE_WORDS',_binary '\0cHx—Ø€\0'),(_binary '\0c\0STWT_SUSPICIOUS_AUTH_ORIGIN',_binary '\0cHx—\Ó R\0'),(_binary '\0c\0STWT_SUSPICIOUS_MDN',_binary '\0cHx—\Ò\àP\0'),(_binary '\0c\0STWT_SVC_OR_TAGGED',_binary '\0cHx—\Ô@`\0'),(_binary '\0c\0STWT_TAGGED_RCPT',_binary '\0cHx—\Ø\0Œ\0'),(_binary '\0c\0STWT_THREAD_HIJACKING',_binary '\0cHx—\Ò@H\0'),(_binary '\0c\0STWT_TO_DN_RECIPIENTS',_binary '\0cHx—\×Àˆ\0'),(_binary '\0c\0STWT_TRUSTED_DOMAIN',_binary '\0cHx—\Õ@l\0'),(_binary '\0c\0STWT_UNDISC_RCPTS_BULK',_binary '\0cHx—\Ğ`4\0'),(_binary '\0c\0STWT_UNITEDINTERNET_SPAM',_binary '\0cHx—Ö |\0'),(_binary '\0c\0STWT_URI_HIDDEN_PATH',_binary '\0cHx—\Ú@¤\0'),(_binary '\0c\0STWT_VIOLATED_DIRECT_SPF',_binary '\0cHx—Ñ B\0'),(_binary '\0c\0STWT_WP_COMPROMISED',_binary '\0cHx—\Ú\0¢\0'),(_binary '\0c\0STWT_WWW_DOMAIN',_binary '\0cHx—\Ô`b\0'),(_binary '\0c\0STWT_XM_CASE',_binary '\0cHx—\×@‚\0'),(_binary '\0c\0STWT_XM_UA_NO_VERSION',_binary '\0cHx—\×`„\0'),(_binary '\0c\0STWT_X_PHP_EVAL',_binary '\0cHx—\Ö`x\0'),(_binary '\0e\ìABUSE_FROM_INJECTOR',_binary '\0eHx—İ \Ê\0'),(_binary '\0e\ìABUSE_SURBL',_binary '\0eHx—\İ\à\Ì\0'),(_binary '\0e\ìARC_ALLOW',_binary '\0eHx—\Ş\0\Î\0'),(_binary '\0e\ìARC_DNSFAIL',_binary '\0eHx—\Ş \Ğ\0'),(_binary '\0e\ìARC_INVALID',_binary '\0eHx—\Ş`\Ò\0'),(_binary '\0e\ìARC_NA',_binary '\0eHx—Ş \Ô\0'),(_binary '\0e\ìARC_REJECT',_binary '\0eHx—\ŞÀ\Ö\0'),(_binary '\0e\ìARC_SIGNED',_binary '\0eHx—\Ş\à\Ø\0'),(_binary '\0e\ìAUTH_NA',_binary '\0eHx—\ß\0\Ú\0'),(_binary '\0e\ìAUTH_NA_OR_FAIL',_binary '\0eHx—\ß \Ü\0'),(_binary '\0e\ìAUTOGEN_PHP_SPAMMY',_binary '\0eHx—\ß@\Ş\0'),(_binary '\0e\ìBAD_CTE_7BIT',_binary '\0eHx—\ß`\à\0'),(_binary '\0e\ìBLOCKED_DOMAIN',_binary '\0eHx—ß \â\0'),(_binary '\0e\ìBODY_URI_ONLY',_binary '\0eHx—\ßÀ\ä\0'),(_binary '\0e\ìBOGUS_ENCRYPTED_AND_TEXT',_binary '\0eHx—\ß\à\æ\0'),(_binary '\0e\ìBOUNCE',_binary '\0eHx—\à \è\0'),(_binary '\0e\ìBOUNCE_NO_AUTH',_binary '\0eHx—\à@\ê\0'),(_binary '\0e\ìBROKEN_CONTENT_TYPE',_binary '\0eHx—\à€\ì\0'),(_binary '\0e\ìCOMPLETELY_EMPTY',_binary '\0eHx—\à \î\0'),(_binary '\0e\ìCOMPROMISED_ACCT_BULK',_binary '\0eHx—\àÀ\ğ\0'),(_binary '\0e\ìCRACKED_SURBL',_binary '\0eHx—\á\0\ò\0'),(_binary '\0e\ìCTE_CASE',_binary '\0eHx—\á@\ô\0'),(_binary '\0e\ìCTYPE_MISSING_DISPOSITION',_binary '\0eHx—\á`\ö\0'),(_binary '\0e\ìCTYPE_MIXED_BOGUS',_binary '\0eHx—\á€ø\0'),(_binary '\0e\ìCT_EXTRA_SEMI',_binary '\0eHx—\á ú\0'),(_binary '\0e\ìDATA_URI_OBFU',_binary '\0eHx—\áÀü\0'),(_binary '\0e\ìDATE_IN_FUTURE',_binary '\0eHx—\â\0ş\0'),(_binary '\0e\ìDATE_IN_PAST',_binary '\0eHx—\âA\0\0'),(_binary '\0e\ìDBL_ABUSE',_binary '\0eHx—\âa\0'),(_binary '\0e\ìDBL_ABUSE_BOTNET',_binary '\0eHx—\â\0'),(_binary '\0e\ìDBL_ABUSE_MALWARE',_binary '\0eHx—\âÁ\0'),(_binary '\0e\ìDBL_ABUSE_PHISH',_binary '\0eHx—\â\á\0'),(_binary '\0e\ìDBL_ABUSE_REDIR',_binary '\0eHx—\ã\n\0'),(_binary '\0e\ìDBL_BLOCKED',_binary '\0eHx—\ã!\0'),(_binary '\0e\ìDBL_BLOCKED_OPENRESOLVER',_binary '\0eHx—\ãa\0'),(_binary '\0e\ìDBL_BOTNET',_binary '\0eHx—\ã\0'),(_binary '\0e\ìDBL_MALWARE',_binary '\0eHx—\ãÁ\0'),(_binary '\0e\ìDBL_PHISH',_binary '\0eHx—\ã\á\0'),(_binary '\0e\ìDBL_SPAM',_binary '\0eHx—\ä\0'),(_binary '\0e\ìDCC_BULK',_binary '\0eHx—\êA\0'),(_binary '\0e\ìDIRECT_TO_MX',_binary '\0eHx—\ê\Z\0'),(_binary '\0e\ìDISPOSABLE_CC',_binary '\0eHx—\ê¡\0'),(_binary '\0e\ìDISPOSABLE_DNT',_binary '\0eHx—\êÁ\0'),(_binary '\0e\ìDISPOSABLE_ENV_FROM',_binary '\0eHx—\ê\á \0'),(_binary '\0e\ìDISPOSABLE_FROM',_binary '\0eHx—\ë!\"\0'),(_binary '\0e\ìDISPOSABLE_REPLYTO',_binary '\0eHx—\ëA$\0'),(_binary '\0e\ìDISPOSABLE_TO',_binary '\0eHx—\ëa&\0'),(_binary '\0e\ìDKIM_ALLOW',_binary '\0eHx—\ë(\0'),(_binary '\0e\ìDKIM_NA',_binary '\0eHx—\ë¡*\0'),(_binary '\0e\ìDKIM_PERMFAIL',_binary '\0eHx—\ëÁ,\0'),(_binary '\0e\ìDKIM_REJECT',_binary '\0eHx—\ë\á.\0'),(_binary '\0e\ìDKIM_SIGNED',_binary '\0eHx—\ì0\0'),(_binary '\0e\ìDKIM_TEMPFAIL',_binary '\0eHx—\ìA2\0'),(_binary '\0e\ìDMARC_BAD_POLICY',_binary '\0eHx—\ìa4\0'),(_binary '\0e\ìDMARC_DNSFAIL',_binary '\0eHx—\ì6\0'),(_binary '\0e\ìDMARC_NA',_binary '\0eHx—\ì¡8\0'),(_binary '\0e\ìDMARC_POLICY_ALLOW',_binary '\0eHx—\ìÁ:\0'),(_binary '\0e\ìDMARC_POLICY_ALLOW_WITH_FAILURES',_binary '\0eHx—\ìÁ<\0'),(_binary '\0e\ìDMARC_POLICY_QUARANTINE',_binary '\0eHx—\ì\á>\0'),(_binary '\0e\ìDMARC_POLICY_REJECT',_binary '\0eHx—\í!@\0'),(_binary '\0e\ìDMARC_POLICY_SOFTFAIL',_binary '\0eHx—\íAB\0'),(_binary '\0e\ìDNSWL_BLOCKED',_binary '\0eHx—\íaD\0'),(_binary '\0e\ìDWL_DNSWL_BLOCKED',_binary '\0eHx—\í¡F\0'),(_binary '\0e\ìDWL_DNSWL_HI',_binary '\0eHx—\íÁH\0'),(_binary '\0e\ìDWL_DNSWL_LOW',_binary '\0eHx—\í\áJ\0'),(_binary '\0e\ìDWL_DNSWL_MED',_binary '\0eHx—\í\áL\0'),(_binary '\0e\ìDWL_DNSWL_NONE',_binary '\0eHx—\îAN\0'),(_binary '\0e\ìEMPTY_SUBJECT',_binary '\0eHx—\îaP\0'),(_binary '\0e\ìENCRYPTED_PGP',_binary '\0eHx—\îR\0'),(_binary '\0e\ìENCRYPTED_SMIME',_binary '\0eHx—\î¡T\0'),(_binary '\0e\ìENV_FROM_INVALID',_binary '\0eHx—\î¡V\0'),(_binary '\0e\ìEXT_CSS',_binary '\0eHx—\îÁX\0'),(_binary '\0e\ìFAKE_REPLY',_binary '\0eHx—\î\áZ\0'),(_binary '\0e\ìFORGED_RCVD_TRAIL',_binary '\0eHx—\ïA\\\0'),(_binary '\0e\ìFORGED_RECIPIENTS',_binary '\0eHx—\ïa^\0'),(_binary '\0e\ìFORGED_RECIPIENTS_MAILLIST',_binary '\0eHx—\ï¡`\0'),(_binary '\0e\ìFORGED_SENDER',_binary '\0eHx—\ïÁb\0'),(_binary '\0e\ìFORGED_SENDER_MAILLIST',_binary '\0eHx—\ğd\0'),(_binary '\0e\ìFREEMAIL_AFF',_binary '\0eHx—\ğ!f\0'),(_binary '\0e\ìFREEMAIL_CC',_binary '\0eHx—\ğAh\0'),(_binary '\0e\ìFREEMAIL_DNT',_binary '\0eHx—\ğaj\0'),(_binary '\0e\ìFREEMAIL_ENV_FROM',_binary '\0eHx—\ğl\0'),(_binary '\0e\ìFREEMAIL_FROM',_binary '\0eHx—\ğ¡n\0'),(_binary '\0e\ìFREEMAIL_REPLY_TO',_binary '\0eHx—\ğÁp\0'),(_binary '\0e\ìFREEMAIL_REPLY_TO_NEQ_FROM_DOM',_binary '\0eHx—\ğ\ár\0'),(_binary '\0e\ìFREEMAIL_TO',_binary '\0eHx—\ñt\0'),(_binary '\0e\ìFROMHOST_NORES_A_OR_MX',_binary '\0eHx—\ñ!v\0'),(_binary '\0e\ìFROM_BOUNCE',_binary '\0eHx—\ñ!x\0'),(_binary '\0e\ìFROM_DN_EQ_ADDR',_binary '\0eHx—\ñAz\0'),(_binary '\0e\ìFROM_EQ_ENV_FROM',_binary '\0eHx—\ñ|\0'),(_binary '\0e\ìFROM_EXCESS_BASE64',_binary '\0eHx—\ñÁ~\0'),(_binary '\0e\ìFROM_EXCESS_QP',_binary '\0eHx—\ñ\á€\0'),(_binary '\0e\ìFROM_HAS_DN',_binary '\0eHx—\ò‚\0'),(_binary '\0e\ìFROM_INVALID',_binary '\0eHx—\ò!„\0'),(_binary '\0e\ìFROM_NAME_EXCESS_SPACE',_binary '\0eHx—\òA†\0'),(_binary '\0e\ìFROM_NAME_HAS_TITLE',_binary '\0eHx—\òaˆ\0'),(_binary '\0e\ìFROM_NEEDS_ENCODING',_binary '\0eHx—\òŠ\0'),(_binary '\0e\ìFROM_NEQ_DISPLAY_NAME',_binary '\0eHx—\òÁŒ\0'),(_binary '\0e\ìFROM_NEQ_ENV_FROM',_binary '\0eHx—\ò\á\0'),(_binary '\0e\ìFROM_NO_DN',_binary '\0eHx—\ó\0'),(_binary '\0e\ìFROM_SERVICE_ACCT',_binary '\0eHx—\óA’\0'),(_binary '\0e\ìGTUBE_TEST',_binary '\0eHx—\óa”\0'),(_binary '\0e\ìHACKED_WP_PHISHING',_binary '\0eHx—\ó–\0'),(_binary '\0e\ìHAS_ANON_DOMAIN',_binary '\0eHx—\ó¡˜\0'),(_binary '\0e\ìHAS_ATTACHMENT',_binary '\0eHx—\óÁš\0'),(_binary '\0e\ìHAS_DATA_URI',_binary '\0eHx—\ó\áœ\0'),(_binary '\0e\ìHAS_GOOGLE_FIREBASE_URL',_binary '\0eHx—\ô\0'),(_binary '\0e\ìHAS_GOOGLE_REDIR',_binary '\0eHx—\ôa \0'),(_binary '\0e\ìHAS_GUC_PROXY_URI',_binary '\0eHx—\ô¢\0'),(_binary '\0e\ìHAS_IPFS_GATEWAY_URL',_binary '\0eHx—\ô¡¤\0'),(_binary '\0e\ìHAS_LIST_UNSUB',_binary '\0eHx—\ôÁ¦\0'),(_binary '\0e\ìHAS_ONION_URI',_binary '\0eHx—\õ¨\0'),(_binary '\0e\ìHAS_PHPMAILER_SIG',_binary '\0eHx—\õ!ª\0'),(_binary '\0e\ìHAS_REPLYTO',_binary '\0eHx—\õA¬\0'),(_binary '\0e\ìHAS_SEO_WORD',_binary '\0eHx—\õa®\0'),(_binary '\0e\ìHAS_WP_URI',_binary '\0eHx—\õ°\0'),(_binary '\0e\ìHAS_X_AS',_binary '\0eHx—\õ¡²\0'),(_binary '\0e\ìHAS_X_GMSV',_binary '\0eHx—\õ\á´\0'),(_binary '\0e\ìHAS_X_PRIO_FIVE',_binary '\0eHx—\ö¶\0'),(_binary '\0e\ìHAS_X_PRIO_ONE',_binary '\0eHx—\ö!¸\0'),(_binary '\0e\ìHAS_X_PRIO_THREE',_binary '\0eHx—\öAº\0'),(_binary '\0e\ìHAS_X_PRIO_TWO',_binary '\0eHx—\öa¼\0'),(_binary '\0e\ìHAS_X_PRIO_ZERO',_binary '\0eHx—\ö¡¾\0'),(_binary '\0e\ìHEADER_EMPTY_DELIMITER',_binary '\0eHx—\öÁÀ\0'),(_binary '\0e\ìHEADER_FORGED_MDN',_binary '\0eHx—\ö\á\Â\0'),(_binary '\0e\ìHEADER_RCONFIRM_MISMATCH',_binary '\0eHx—\÷A\Ä\0'),(_binary '\0e\ìHELO_BAREIP',_binary '\0eHx—\÷a\Æ\0'),(_binary '\0e\ìHELO_IPREV_MISMATCH',_binary '\0eHx—\÷\È\0'),(_binary '\0e\ìHELO_IP_A',_binary '\0eHx—\÷¡\Ê\0'),(_binary '\0e\ìHELO_NORES_A_OR_MX',_binary '\0eHx—\÷¡\Ì\0'),(_binary '\0e\ìHELO_NOT_FQDN',_binary '\0eHx—\÷Á\Î\0'),(_binary '\0e\ìHIDDEN_SOURCE_OBJ',_binary '\0eHx—\÷\á\Ğ\0'),(_binary '\0e\ìHOMOGRAPH_URL',_binary '\0eHx—ø\Ò\0'),(_binary '\0e\ìHTML_META_REFRESH_URL',_binary '\0eHx—ø!\Ô\0'),(_binary '\0e\ìHTML_SHORT_LINK_IMG_1',_binary '\0eHx—øa\Ö\0'),(_binary '\0e\ìHTML_SHORT_LINK_IMG_2',_binary '\0eHx—ø\Ø\0'),(_binary '\0e\ìHTML_SHORT_LINK_IMG_3',_binary '\0eHx—ø¡\Ú\0'),(_binary '\0e\ìHTML_TEXT_IMG_RATIO',_binary '\0eHx—øÁ\Ü\0'),(_binary '\0e\ìHTML_UNBALANCED_TAG',_binary '\0eHx—ø\á\Ş\0'),(_binary '\0e\ìHTTP_TO_HTTPS',_binary '\0eHx—ù\à\0'),(_binary '\0e\ìHTTP_TO_IP',_binary '\0eHx—ù!\â\0'),(_binary '\0e\ìINFO_TO_INFO_LU',_binary '\0eHx—ùa\ä\0'),(_binary '\0e\ìINVALID_DATE',_binary '\0eHx—ùa\æ\0'),(_binary '\0e\ìINVALID_FROM_8BIT',_binary '\0eHx—ù\è\0'),(_binary '\0e\ìINVALID_MSGID',_binary '\0eHx—ù¡\ê\0'),(_binary '\0e\ìKLMS_SPAM',_binary '\0eHx—ùÁ\ì\0'),(_binary '\0e\ìLLM_COMMERCIAL_HIGH',_binary '\0eHx—ù\á\î\0'),(_binary '\0e\ìLLM_COMMERCIAL_LOW',_binary '\0eHx—ú\ğ\0'),(_binary '\0e\ìLLM_COMMERCIAL_MEDIUM',_binary '\0eHx—úA\ò\0'),(_binary '\0e\ìLLM_HARMFUL_HIGH',_binary '\0eHx—ú\ô\0'),(_binary '\0e\ìLLM_HARMFUL_LOW',_binary '\0eHx—ú¡\ö\0'),(_binary '\0e\ìLLM_HARMFUL_MEDIUM',_binary '\0eHx—ú\áø\0'),(_binary '\0e\ìLLM_LEGITIMATE_HIGH',_binary '\0eHx—û!ú\0'),(_binary '\0e\ìLLM_LEGITIMATE_LOW',_binary '\0eHx—ûAü\0'),(_binary '\0e\ìLLM_LEGITIMATE_MEDIUM',_binary '\0eHx—ûaş\0'),(_binary '\0e\ìLLM_UNSOLICITED_HIGH',_binary '\0eHx—û\Â\0\0'),(_binary '\0e\ìLLM_UNSOLICITED_LOW',_binary '\0eHx—û\â\0'),(_binary '\0e\ìLLM_UNSOLICITED_MEDIUM',_binary '\0eHx—û\â\0'),(_binary '\0e\ìLONG_SUBJ',_binary '\0eHx—ü\0'),(_binary '\0e\ìMAILLIST',_binary '\0eHx—ü\"\0'),(_binary '\0e\ìMANY_INVISIBLE_PARTS',_binary '\0eHx—üB\n\0'),(_binary '\0e\ìMID_BARE_IP',_binary '\0eHx—üb\0'),(_binary '\0e\ìMID_CONTAINS_FROM',_binary '\0eHx—ü‚\0'),(_binary '\0e\ìMID_CONTAINS_TO',_binary '\0eHx—ü\â\0'),(_binary '\0e\ìMID_MISSING_BRACKETS',_binary '\0eHx—ü\â\0'),(_binary '\0e\ìMID_RHS_IP_LITERAL',_binary '\0eHx—ı\0'),(_binary '\0e\ìMID_RHS_MATCH_FROM',_binary '\0eHx—ı\"\0'),(_binary '\0e\ìMID_RHS_MATCH_FROMTLD',_binary '\0eHx—ıB\0'),(_binary '\0e\ìMID_RHS_MATCH_TO',_binary '\0eHx—ıb\Z\0'),(_binary '\0e\ìMID_RHS_NOT_FQDN',_binary '\0eHx—ı‚\0'),(_binary '\0e\ìMID_RHS_WWW',_binary '\0eHx—ı¢\0'),(_binary '\0e\ìMIME_ARCHIVE_IN_ARCHIVE',_binary '\0eHx—ı\Â \0'),(_binary '\0e\ìMIME_BAD',_binary '\0eHx—ı\â\"\0'),(_binary '\0e\ìMIME_BAD_ATTACHMENT',_binary '\0eHx—ş$\0'),(_binary '\0e\ìMIME_BAD_EXTENSION',_binary '\0eHx—şB&\0'),(_binary '\0e\ìMIME_BAD_EXT_WITH_BAD_UNICODE',_binary '\0eHx—şb(\0'),(_binary '\0e\ìMIME_BAD_UNICODE',_binary '\0eHx—ş‚*\0'),(_binary '\0e\ìMIME_BASE64_TEXT',_binary '\0eHx—ş¢,\0'),(_binary '\0e\ìMIME_BASE64_TEXT_BOGUS',_binary '\0eHx—ş\Â.\0'),(_binary '\0e\ìMIME_DOUBLE_BAD_EXTENSION',_binary '\0eHx—ş\â0\0'),(_binary '\0e\ìMIME_GOOD',_binary '\0eHx—ÿ2\0'),(_binary '\0e\ìMIME_HEADER_CTYPE_ONLY',_binary '\0eHx—ÿ\"4\0'),(_binary '\0e\ìMIME_HTML_ONLY',_binary '\0eHx—ÿb6\0'),(_binary '\0e\ìMIME_MA_MISSING_HTML',_binary '\0eHx—ÿ‚8\0'),(_binary '\0e\ìMIME_MA_MISSING_TEXT',_binary '\0eHx—ÿ¢:\0'),(_binary '\0e\ìMISSING_CHARSET',_binary '\0eHx—ÿ\Â<\0'),(_binary '\0e\ìMISSING_DATE',_binary '\0eHx—ÿ\â>\0'),(_binary '\0e\ìMISSING_ESSENTIAL_HEADERS',_binary '\0eHx˜\0@\0'),(_binary '\0e\ìMISSING_FROM',_binary '\0eHx˜\0\"B\0'),(_binary '\0e\ìMISSING_MID',_binary '\0eHx˜\0BD\0'),(_binary '\0e\ìMISSING_MIME_VERSION',_binary '\0eHx˜\0‚F\0'),(_binary '\0e\ìMISSING_SUBJECT',_binary '\0eHx˜\0¢H\0'),(_binary '\0e\ìMISSING_TO',_binary '\0eHx˜\0\ÂJ\0'),(_binary '\0e\ìMIXED_CHARSET',_binary '\0eHx˜\0\âL\0'),(_binary '\0e\ìMIXED_CHARSET_URL',_binary '\0eHx˜N\0'),(_binary '\0e\ìMSBL_EBL',_binary '\0eHx˜\"P\0'),(_binary '\0e\ìMSBL_EBL_GREY',_binary '\0eHx˜BR\0'),(_binary '\0e\ìMULTIPLE_FROM',_binary '\0eHx˜bT\0'),(_binary '\0e\ìMULTIPLE_UNIQUE_HEADERS',_binary '\0eHx˜¢V\0'),(_binary '\0e\ìMV_CASE',_binary '\0eHx˜\ÂX\0'),(_binary '\0e\ìMW_SURBL_MULTI',_binary '\0eHx˜\âZ\0'),(_binary '\0e\ìNO_SPACE_IN_FROM',_binary '\0eHx˜\\\0'),(_binary '\0e\ìPARTS_DIFFER',_binary '\0eHx˜\"^\0'),(_binary '\0e\ìPHISHED_OPENPHISH',_binary '\0eHx˜\"`\0'),(_binary '\0e\ìPHISHED_PHISHTANK',_binary '\0eHx˜Bb\0'),(_binary '\0e\ìPHISHING',_binary '\0eHx˜bd\0'),(_binary '\0e\ìPHISH_EMOTION',_binary '\0eHx˜¢f\0'),(_binary '\0e\ìPH_SURBL_MULTI',_binary '\0eHx˜¢h\0'),(_binary '\0e\ìPRECEDENCE_BULK',_binary '\0eHx˜\âj\0'),(_binary '\0e\ìPREVIOUSLY_DELIVERED',_binary '\0eHx˜\"l\0'),(_binary '\0e\ìPROB_HAM_HIGH',_binary '\0eHx˜bn\0'),(_binary '\0e\ìPROB_HAM_LOW',_binary '\0eHx˜‚p\0'),(_binary '\0e\ìPROB_HAM_MEDIUM',_binary '\0eHx˜¢r\0'),(_binary '\0e\ìPROB_SPAM_HIGH',_binary '\0eHx˜\Ât\0'),(_binary '\0e\ìPROB_SPAM_LOW',_binary '\0eHx˜v\0'),(_binary '\0e\ìPROB_SPAM_MEDIUM',_binary '\0eHx˜\"x\0'),(_binary '\0e\ìPROB_SPAM_UNCERTAIN',_binary '\0eHx˜Bz\0'),(_binary '\0e\ìPYZOR',_binary '\0eHx˜b|\0'),(_binary '\0e\ìRBL_BARRACUDA',_binary '\0eHx˜‚~\0'),(_binary '\0e\ìRBL_BLOCKLISTDE',_binary '\0eHx˜¢€\0'),(_binary '\0e\ìRBL_MAILSPIKE_BAD',_binary '\0eHx˜Â‚\0'),(_binary '\0e\ìRBL_MAILSPIKE_VERYBAD',_binary '\0eHx˜„\0'),(_binary '\0e\ìRBL_MAILSPIKE_WORST',_binary '\0eHx˜\"†\0'),(_binary '\0e\ìRBL_SEM',_binary '\0eHx˜Bˆ\0'),(_binary '\0e\ìRBL_SEM_IPV6',_binary '\0eHx˜bŠ\0'),(_binary '\0e\ìRBL_SENDERSCORE_BLOCKED',_binary '\0eHx˜‚Œ\0'),(_binary '\0e\ìRBL_SENDERSCORE_BOT',_binary '\0eHx˜¢\0'),(_binary '\0e\ìRBL_SENDERSCORE_NA',_binary '\0eHx˜Â\0'),(_binary '\0e\ìRBL_SENDERSCORE_NA_BOT',_binary '\0eHx˜\â’\0'),(_binary '\0e\ìRBL_SENDERSCORE_PRST',_binary '\0eHx˜”\0'),(_binary '\0e\ìRBL_SENDERSCORE_PRST_BOT',_binary '\0eHx˜B–\0'),(_binary '\0e\ìRBL_SENDERSCORE_PRST_NA',_binary '\0eHx˜b˜\0'),(_binary '\0e\ìRBL_SENDERSCORE_PRST_NA_BOT',_binary '\0eHx˜‚š\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_0',_binary '\0eHx˜¢œ\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_1',_binary '\0eHx˜Â\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_2',_binary '\0eHx˜\â \0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_3',_binary '\0eHx˜\"¢\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_4',_binary '\0eHx˜B¤\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_5',_binary '\0eHx˜b¦\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_6',_binary '\0eHx˜‚¨\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_7',_binary '\0eHx˜¢ª\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_8',_binary '\0eHx˜Â¬\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_9',_binary '\0eHx˜\â®\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_BLOCKED',_binary '\0eHx˜°\0'),(_binary '\0e\ìRBL_SENDERSCORE_REPUT_UNKNOWN',_binary '\0eHx˜\"²\0'),(_binary '\0e\ìRBL_SENDERSCORE_SCORE',_binary '\0eHx˜B´\0'),(_binary '\0e\ìRBL_SENDERSCORE_SCORE_NA',_binary '\0eHx˜‚¶\0'),(_binary '\0e\ìRBL_SENDERSCORE_SCORE_PRST',_binary '\0eHx˜‚¸\0'),(_binary '\0e\ìRBL_SENDERSCORE_SCORE_PRST_NA',_binary '\0eHx˜¢º\0'),(_binary '\0e\ìRBL_SENDERSCORE_SCORE_SUS_ATT_NA',_binary '\0eHx˜Â¼\0'),(_binary '\0e\ìRBL_SENDERSCORE_SUS_ATT',_binary '\0eHx˜\â¾\0'),(_binary '\0e\ìRBL_SENDERSCORE_SUS_ATT_NA',_binary '\0eHx˜	À\0'),(_binary '\0e\ìRBL_SENDERSCORE_SUS_ATT_NA_BOT',_binary '\0eHx˜	\"\Â\0'),(_binary '\0e\ìRBL_SENDERSCORE_SUS_ATT_PRST_NA',_binary '\0eHx˜	b\Ä\0'),(_binary '\0e\ìRBL_SENDERSCORE_SUS_ATT_PRST_NA_BOT',_binary '\0eHx˜	‚\Æ\0'),(_binary '\0e\ìRBL_SPAMCOP',_binary '\0eHx˜	¢\È\0'),(_binary '\0e\ìRBL_SPAMHAUS_BLOCKED',_binary '\0eHx˜	\Â\Ê\0'),(_binary '\0e\ìRBL_SPAMHAUS_BLOCKED_OPENRESOLVER',_binary '\0eHx˜	\â\Ì\0'),(_binary '\0e\ìRBL_SPAMHAUS_CSS',_binary '\0eHx˜\n\Î\0'),(_binary '\0e\ìRBL_SPAMHAUS_DROP',_binary '\0eHx˜\n\"\Ğ\0'),(_binary '\0e\ìRBL_SPAMHAUS_PBL',_binary '\0eHx˜\nB\Ò\0'),(_binary '\0e\ìRBL_SPAMHAUS_SBL',_binary '\0eHx˜\nb\Ô\0'),(_binary '\0e\ìRBL_SPAMHAUS_XBL',_binary '\0eHx˜\n‚\Ö\0'),(_binary '\0e\ìRBL_VIRUSFREE_BOTNET',_binary '\0eHx˜\n\Â\Ø\0'),(_binary '\0e\ìRCPT_BOUNCEMOREONE',_binary '\0eHx˜\n\â\Ú\0'),(_binary '\0e\ìRCPT_COUNT_FIVE',_binary '\0eHx˜\Ü\0'),(_binary '\0e\ìRCPT_COUNT_GT_50',_binary '\0eHx˜\"\Ş\0'),(_binary '\0e\ìRCPT_COUNT_ONE',_binary '\0eHx˜B\à\0'),(_binary '\0e\ìRCPT_COUNT_SEVEN',_binary '\0eHx˜b\â\0'),(_binary '\0e\ìRCPT_COUNT_THREE',_binary '\0eHx˜‚\ä\0'),(_binary '\0e\ìRCPT_COUNT_TWELVE',_binary '\0eHx˜\Â\æ\0'),(_binary '\0e\ìRCPT_COUNT_TWO',_binary '\0eHx˜\â\è\0'),(_binary '\0e\ìRCPT_COUNT_ZERO',_binary '\0eHx˜\ê\0'),(_binary '\0e\ìRCPT_DOMAIN_IN_MESSAGE',_binary '\0eHx˜\"\ì\0'),(_binary '\0e\ìRCPT_DOMAIN_IN_SUBJECT',_binary '\0eHx˜B\î\0'),(_binary '\0e\ìRCPT_IN_SUBJECT',_binary '\0eHx˜‚\ğ\0'),(_binary '\0e\ìRCPT_LOCAL_IN_SUBJECT',_binary '\0eHx˜\Â\ò\0'),(_binary '\0e\ìRCVD_COUNT_FIVE',_binary '\0eHx˜\Â\ô\0'),(_binary '\0e\ìRCVD_COUNT_ONE',_binary '\0eHx˜\â\ö\0'),(_binary '\0e\ìRCVD_COUNT_SEVEN',_binary '\0eHx˜\r\"ø\0'),(_binary '\0e\ìRCVD_COUNT_THREE',_binary '\0eHx˜\rBú\0'),(_binary '\0e\ìRCVD_COUNT_TWELVE',_binary '\0eHx˜\r‚ü\0'),(_binary '\0e\ìRCVD_COUNT_TWO',_binary '\0eHx˜\r¢ş\0'),(_binary '\0e\ìRCVD_COUNT_ZERO',_binary '\0eHx˜\r\Ã\0\0'),(_binary '\0e\ìRCVD_DKIM_ARC_DNSWL_HI',_binary '\0eHx˜\r\Ã\0'),(_binary '\0e\ìRCVD_DKIM_ARC_DNSWL_MED',_binary '\0eHx˜#\0'),(_binary '\0e\ìRCVD_DOUBLE_IP_SPAM',_binary '\0eHx˜C\0'),(_binary '\0e\ìRCVD_HELO_USER',_binary '\0eHx˜c\0'),(_binary '\0e\ìRCVD_ILLEGAL_CHARS',_binary '\0eHx˜c\n\0'),(_binary '\0e\ìRCVD_IN_DNSWL_HI',_binary '\0eHx˜ƒ\0'),(_binary '\0e\ìRCVD_IN_DNSWL_LOW',_binary '\0eHx˜£\0'),(_binary '\0e\ìRCVD_IN_DNSWL_MED',_binary '\0eHx˜\Ã\0'),(_binary '\0e\ìRCVD_IN_DNSWL_NONE',_binary '\0eHx˜#\0'),(_binary '\0e\ìRCVD_NO_TLS_LAST',_binary '\0eHx˜C\0'),(_binary '\0e\ìRCVD_TLS_ALL',_binary '\0eHx˜ƒ\0'),(_binary '\0e\ìRCVD_TLS_LAST',_binary '\0eHx˜ƒ\0'),(_binary '\0e\ìRCVD_UNAUTH_PBL',_binary '\0eHx˜£\Z\0'),(_binary '\0e\ìRCVD_VIA_SMTP_AUTH',_binary '\0eHx˜\Ã\0'),(_binary '\0e\ìRDNS_DNSFAIL',_binary '\0eHx˜\0'),(_binary '\0e\ìRDNS_NONE',_binary '\0eHx˜# \0'),(_binary '\0e\ìRECEIVED_BLOCKLISTDE',_binary '\0eHx˜C\"\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_BLOCKED',_binary '\0eHx˜c$\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_BLOCKED_OPENRESOLVER',_binary '\0eHx˜ƒ&\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_CSS',_binary '\0eHx˜\Ã(\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_PBL',_binary '\0eHx˜\ã*\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_SBL',_binary '\0eHx˜,\0'),(_binary '\0e\ìRECEIVED_SPAMHAUS_XBL',_binary '\0eHx˜#.\0'),(_binary '\0e\ìREDIRECTOR_URL',_binary '\0eHx˜C0\0'),(_binary '\0e\ìREDIRECTOR_URL_ONLY',_binary '\0eHx˜ƒ2\0'),(_binary '\0e\ìREPLYTO_ADDR_EQ_FROM',_binary '\0eHx˜£4\0'),(_binary '\0e\ìREPLYTO_DN_EQ_FROM_DN',_binary '\0eHx˜\Ã6\0'),(_binary '\0e\ìREPLYTO_DOM_EQ_FROM_DOM',_binary '\0eHx˜\ã8\0'),(_binary '\0e\ìREPLYTO_DOM_NEQ_FROM_DOM',_binary '\0eHx˜:\0'),(_binary '\0e\ìREPLYTO_EMAIL_HAS_TITLE',_binary '\0eHx˜C<\0'),(_binary '\0e\ìREPLYTO_EQ_FROM',_binary '\0eHx˜c>\0'),(_binary '\0e\ìREPLYTO_EQ_TO_ADDR',_binary '\0eHx˜ƒ@\0'),(_binary '\0e\ìREPLYTO_EXCESS_BASE64',_binary '\0eHx˜£B\0'),(_binary '\0e\ìREPLYTO_EXCESS_QP',_binary '\0eHx˜\ãD\0'),(_binary '\0e\ìREPLYTO_UNPARSABLE',_binary '\0eHx˜\ãF\0'),(_binary '\0e\ìRWL_MAILSPIKE_EXCELLENT',_binary '\0eHx˜H\0'),(_binary '\0e\ìRWL_MAILSPIKE_GOOD',_binary '\0eHx˜#J\0'),(_binary '\0e\ìRWL_MAILSPIKE_NEUTRAL',_binary '\0eHx˜CL\0'),(_binary '\0e\ìRWL_MAILSPIKE_POSSIBLE',_binary '\0eHx˜£N\0'),(_binary '\0e\ìRWL_MAILSPIKE_VERYGOOD',_binary '\0eHx˜\ÃP\0'),(_binary '\0e\ìSEM_URIBL',_binary '\0eHx˜\ÃR\0'),(_binary '\0e\ìSEM_URIBL_FRESH15',_binary '\0eHx˜\ãT\0'),(_binary '\0e\ìSEO_SPAM',_binary '\0eHx˜V\0'),(_binary '\0e\ìSHORT_PART_BAD_HEADERS',_binary '\0eHx˜#X\0'),(_binary '\0e\ìSIGNED_PGP',_binary '\0eHx˜CZ\0'),(_binary '\0e\ìSIGNED_SMIME',_binary '\0eHx˜£\\\0'),(_binary '\0e\ìSINGLE_SHORT_PART',_binary '\0eHx˜\ã^\0'),(_binary '\0e\ìSORTED_RECIPS',_binary '\0eHx˜`\0'),(_binary '\0e\ìSPAM_FLAG',_binary '\0eHx˜#b\0'),(_binary '\0e\ìSPAM_TRAP',_binary '\0eHx˜Cd\0'),(_binary '\0e\ìSPF_ALLOW',_binary '\0eHx˜cf\0'),(_binary '\0e\ìSPF_DNSFAIL',_binary '\0eHx˜ch\0'),(_binary '\0e\ìSPF_FAIL',_binary '\0eHx˜£j\0'),(_binary '\0e\ìSPF_NA',_binary '\0eHx˜\Ãl\0'),(_binary '\0e\ìSPF_NEUTRAL',_binary '\0eHx˜n\0'),(_binary 'ÿÿ\0\òadmin\0\0\0\0\0\0\0',_binary '\0\0\0\0\0\0\0\0\0'),(_binary 'ÿÿÿÿ\0\0',_binary '\0\0\0\0j0`899e18d22287');
/*!40000 ALTER TABLE `g` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `h`
--

DROP TABLE IF EXISTS `h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `h` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `h`
--

LOCK TABLES `h` WRITE;
/*!40000 ALTER TABLE `h` DISABLE KEYS */;
/*!40000 ALTER TABLE `h` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `i`
--

DROP TABLE IF EXISTS `i`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `i` (
  `k` blob NOT NULL,
  PRIMARY KEY (`k`(400))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `i`
--

LOCK TABLES `i` WRITE;
/*!40000 ALTER TABLE `i` DISABLE KEYS */;
/*!40000 ALTER TABLE `i` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `j`
--

DROP TABLE IF EXISTS `j`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `j` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `j`
--

LOCK TABLES `j` WRITE;
/*!40000 ALTER TABLE `j` DISABLE KEYS */;
/*!40000 ALTER TABLE `j` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `k`
--

DROP TABLE IF EXISTS `k`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `k` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `k`
--

LOCK TABLES `k` WRITE;
/*!40000 ALTER TABLE `k` DISABLE KEYS */;
INSERT INTO `k` VALUES (_binary 'qY&E×†„\Ó\ÅD7qsF>m\çz†İ\0lÁ\Ç\rX+µ',''),(_binary 'qY&E×†„\Ó\ÅD7qsF>m\çz†İ\0lÁ\Ç\rX+µÿÿÿÿ\0\0\0\0j7½^','');
/*!40000 ALTER TABLE `k` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `l`
--

DROP TABLE IF EXISTS `l`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `l` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `l`
--

LOCK TABLES `l` WRITE;
/*!40000 ALTER TABLE `l` DISABLE KEYS */;
/*!40000 ALTER TABLE `l` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `m`
--

DROP TABLE IF EXISTS `m`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `m` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `m`
--

LOCK TABLES `m` WRITE;
/*!40000 ALTER TABLE `m` DISABLE KEYS */;
INSERT INTO `m` VALUES (_binary '\0\0\0\0\0\0\0Ä‰V',_binary '\0\0\0\0\0\0\0\0\0\0\0\0j0d'),(_binary 'Hx—À\0',_binary '\0\0\0\0j>n');
/*!40000 ALTER TABLE `m` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `n`
--

DROP TABLE IF EXISTS `n`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `n` (
  `k` tinyblob NOT NULL,
  `v` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `n`
--

LOCK TABLES `n` WRITE;
/*!40000 ALTER TABLE `n` DISABLE KEYS */;
INSERT INTO `n` VALUES (_binary '\0\0',1),(_binary '\0%',1),(_binary '\0S',4);
/*!40000 ALTER TABLE `n` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `o`
--

DROP TABLE IF EXISTS `o`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `o` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `o`
--

LOCK TABLES `o` WRITE;
/*!40000 ALTER TABLE `o` DISABLE KEYS */;
/*!40000 ALTER TABLE `o` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `p`
--

DROP TABLE IF EXISTS `p`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `p` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `p`
--

LOCK TABLES `p` WRITE;
/*!40000 ALTER TABLE `p` DISABLE KEYS */;
INSERT INTO `p` VALUES (_binary '\0',_binary '\0\0\0');
/*!40000 ALTER TABLE `p` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `q`
--

DROP TABLE IF EXISTS `q`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `q` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `q`
--

LOCK TABLES `q` WRITE;
/*!40000 ALTER TABLE `q` DISABLE KEYS */;
/*!40000 ALTER TABLE `q` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `r`
--

DROP TABLE IF EXISTS `r`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `r` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `r`
--

LOCK TABLES `r` WRITE;
/*!40000 ALTER TABLE `r` DISABLE KEYS */;
/*!40000 ALTER TABLE `r` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `s`
--

DROP TABLE IF EXISTS `s`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `s` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `s`
--

LOCK TABLES `s` WRITE;
/*!40000 ALTER TABLE `s` DISABLE KEYS */;
INSERT INTO `s` VALUES (_binary '\0Hx—	\0\0',_binary '\0Stalwart Web InterfaceHhttps://github.com/stalwartlabs/webui/releases/latest/download/webui.zip/admin/account€û\Ó	\0'),(_binary '\0\0\0CL²M\Í',_binary '\0€¸™)€€€2\à§=https://cdn.jsdelivr.net/npm/@ip-location-db/asn/asn-ipv4.csv=https://cdn.jsdelivr.net/npm/@ip-location-db/asn/asn-ipv6.csvshttps://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-geo-whois-asn-country/geolite2-geo-whois-asn-country-ipv4.csvshttps://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-geo-whois-asn-country/geolite2-geo-whois-asn-country-ipv6.csv\0\0'),(_binary '\0\0\0CL²M\Í',_binary '\0\0\0€\0'),(_binary '\0\0\0CL²M\Í',_binary '/var/lib/stalwart/blobs'),(_binary '\0.\0\0CL²M\Í',_binary '\0\Ğ\à\Ô€€€\à\ÔÀ\îmÀ\îm'),(_binary '\0/\0\0CL²M\Í',_binary '\0\0'),(_binary '\08\0\0CL²M\Í',_binary '\0'),(_binary '\09Hx—\r`',_binary '\0defaultDefault connection strategy\0\0\à§À\Ï$\à§\à§\à§\à§'),(_binary '\0:\0\0\0\0\0\0\0\0',_binary '\0localLocal delivery schedule\0€¨\Ì{\0\0\0'),(_binary '\0:\0\0\0\0\0\0\0',_binary '\0remoteRemote delivery schedule\0€¨\Ì{\0\0'),(_binary '\0:\0\0\0\0\0\0\0',_binary '\0dsn.Delivery Status Notification delivery schedule\n\0 \÷6À\îm€\İ\Û€º·'),(_binary '\0>Hx—	\à',_binary '\0Sender IP throttle\0true\è'),(_binary '\0>Hx—\n ',_binary '\0$Sender address to recipient throttle\0true€\İ\Û'),(_binary '\0CHx—\r ',_binary '\0\0\0mxMX delivery route'),(_binary '\0CHx—\r@',_binary '\0localLocal delivery route'),(_binary '\0KHx—À',_binary '\0invalid-tls\0Allow invalid TLS certificates\0\0\à§ ş\n'),(_binary '\0KHx—\à\n',_binary '\0default\0\0Default TLS settings\0\0\à§ ş\n'),(_binary '\0L\0\0\0\0\0\0\0\0',_binary '\0localLocal delivery queue'),(_binary '\0L\0\0\0\0\0\0\0',_binary '\0remoteRemote delivery queue2'),(_binary '\0L\0\0\0\0\0\0\0',_binary '\0dsn+Delivery Status Notification delivery queue'),(_binary '\0L\0\0\0\0\0\0\0',_binary '\0report#DMARC and TLS report delivery queue'),(_binary '\0MHx—@',_binary '\0smtp\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0€\0\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx—`',_binary '\0submissions\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\Ñ\0\0€\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx—€',_binary '\0imaps\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\á\0€\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx— ',_binary '\0pop3s\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\ã\0€\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx—À\Z',_binary '\0sieve\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\Ş \0€\0\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx—\0',_binary '\0https\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0»\0€\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx— ',_binary '\0http\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0?\0€\0\0\0\0\0\0\0\à\Ô€@'),(_binary '\0MHx—’@ \0',_binary '\0\nimap-plain\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0€\0\0\0\0\0\0\0\0\à\Ô€@'),(_binary '\0O\0\0CL²M\Í',_binary '\0\0\0À\Ï$€û\Ó	€\à\å¤€\İ\ÛÀ\îm \÷6\0@3awRIFhUsf0tnVF8chOgFpsNXAzsVwsq9ACMFk46Nk3GWRGn0Nr3Kq4U0FOpfUIW\0\0\ö-----BEGIN PRIVATE KEY-----\r\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgzYxXhgcT7MWL/znv\r\nehgWpLu1yllb0ro5w+iUnH+Zm9ahRANCAASsdPUoWtGkL4PP+DJGyVnemwDaWNo3\r\nDEdCgC9VJgBRH6eHusYvN1VPZUfOFsuKek3dpprLmBVyrc6FVaGMjrOx\r\n-----END PRIVATE KEY-----\r\n'),(_binary '\0T\0\0CL²M\Í',_binary '\0dgg\0\0	\n\0	\n\0'),(_binary '\0U\0\0CL²M\Í',_binary '\0\0'),(_binary '\0^Hx—Ú€¦\0',_binary '\0octets[0] != 127falseoctets[3] == 2Gif_then(location == \'tcp\', \'RBL_SPAMHAUS_SBL\', \'RECEIVED_SPAMHAUS_SBL\')octets[3] == 3Gif_then(location == \'tcp\', \'RBL_SPAMHAUS_CSS\', \'RECEIVED_SPAMHAUS_CSS\') octets[3] >= 4 && octets[3] <= 7Gif_then(location == \'tcp\', \'RBL_SPAMHAUS_XBL\', \'RECEIVED_SPAMHAUS_XBL\')octets[3] == 9Iif_then(location == \'tcp\', \'RBL_SPAMHAUS_DROP\', \'RECEIVED_SPAMHAUS_DROP\')$(octets[3] == 10 || octets[3] == 11)Gif_then(location == \'tcp\', \'RBL_SPAMHAUS_PBL\', \'RECEIVED_SPAMHAUS_PBL\')octets[3] == 254iif_then(location == \'tcp\', \'RBL_SPAMHAUS_BLOCKED_OPENRESOLVER\', \'RECEIVED_SPAMHAUS_BLOCKED_OPENRESOLVER\')octets[3] == 255Oif_then(location == \'tcp\', \'RBL_SPAMHAUS_BLOCKED\', \'RECEIVED_SPAMHAUS_BLOCKED\')false\0 ip_reverse + \'.zen.spamhaus.org\'STWT_RBL_SPAMHAUS_IP\0'),(_binary '\0^Hx—\ÚÀ¨\0',_binary '\0	octets[0] != 127falseoctets[3] == 10\'RBL_MAILSPIKE_WORST\'octets[3] == 11\'RBL_MAILSPIKE_VERYBAD\'octets[3] == 12\'RBL_MAILSPIKE_BAD\'\"octets[3] >= 13 && octets[3] <= 16\'RWL_MAILSPIKE_NEUTRAL\'octets[3] == 17\'RWL_MAILSPIKE_POSSIBLE\'octets[3] == 18\'RWL_MAILSPIKE_GOOD\'octets[3] == 19\'RWL_MAILSPIKE_VERYGOOD\'octets[3] == 20\'RWL_MAILSPIKE_EXCELLENT\'falselocation == \'tcp\'!ip_reverse + \'.rep.mailspike.net\'falseSTWT_RBL_MAILSPIKE_IP\0'),(_binary '\0^Hx—\Ú\àª\0',_binary '\0octets[0] != 127falseoctets[3] == 1\'RBL_SENDERSCORE_BOT\'octets[3] == 2\'RBL_SENDERSCORE_NA\'octets[3] == 3\'RBL_SENDERSCORE_NA_BOT\'octets[3] == 4\'RBL_SENDERSCORE_PRST\'octets[3] == 5\Z\'RBL_SENDERSCORE_PRST_BOT\'octets[3] == 6\'RBL_SENDERSCORE_PRST_NA\'octets[3] == 7\'RBL_SENDERSCORE_PRST_NA_BOT\'octets[3] == 8\'RBL_SENDERSCORE_SUS_ATT\'octets[3] == 10\'RBL_SENDERSCORE_SUS_ATT_NA\'octets[3] == 11 \'RBL_SENDERSCORE_SUS_ATT_NA_BOT\'octets[3] == 14!\'RBL_SENDERSCORE_SUS_ATT_PRST_NA\'octets[3] == 15%\'RBL_SENDERSCORE_SUS_ATT_PRST_NA_BOT\'octets[3] == 16\'RBL_SENDERSCORE_SCORE\'octets[3] == 18\Z\'RBL_SENDERSCORE_SCORE_NA\'octets[3] == 20\'RBL_SENDERSCORE_SCORE_PRST\'octets[3] == 22\'RBL_SENDERSCORE_SCORE_PRST_NA\'octets[3] == 26\"\'RBL_SENDERSCORE_SCORE_SUS_ATT_NA\'octets[3] == 255\'RBL_SENDERSCORE_BLOCKED\'falselocation == \'tcp\'(ip_reverse + \'.bl.score.senderscore.com\'falseSTWT_RBL_SENDERSCORE_IP\0\0'),(_binary '\0^Hx—\Û ¬\0',_binary '\0octets[0] != 127falseoctets[3] < 10\'RBL_SENDERSCORE_REPUT_0\'\"octets[3] >= 10 && octets[3] <= 19\'RBL_SENDERSCORE_REPUT_1\'\"octets[3] >= 20 && octets[3] <= 29\'RBL_SENDERSCORE_REPUT_2\'\"octets[3] >= 30 && octets[3] <= 39\'RBL_SENDERSCORE_REPUT_3\'\"octets[3] >= 40 && octets[3] <= 49\'RBL_SENDERSCORE_REPUT_4\'\"octets[3] >= 50 && octets[3] <= 59\'RBL_SENDERSCORE_REPUT_5\'\"octets[3] >= 60 && octets[3] <= 69\'RBL_SENDERSCORE_REPUT_6\'\"octets[3] >= 70 && octets[3] <= 79\'RBL_SENDERSCORE_REPUT_7\'\"octets[3] >= 80 && octets[3] <= 89\'RBL_SENDERSCORE_REPUT_8\'#octets[3] >= 90 && octets[3] <= 100\'RBL_SENDERSCORE_REPUT_9\'octets[3] == 255\'RBL_SENDERSCORE_REPUT_BLOCKED\'falselocation == \'tcp\'%ip_reverse + \'.score.senderscore.com\'falseSTWT_RBL_SENDERSCORE_REPUT_IP\0'),(_binary '\0^Hx—\Û@®\0',_binary '\0\r!is_empty(ip)	\'RBL_SEM\'false\Zlocation == \'tcp\' && is_v4\'ip_reverse + \'.bl.spameatingmonkey.net\'\Zlocation == \'tcp\' && is_v6,ip_reverse + \'.bl.ipv6.spameatingmonkey.net\'falseSTWT_RBL_SEM_IP\0'),(_binary '\0^Hx—\Û`°\0',_binary '\0ip == \'127.0.0.2\'\'RBL_VIRUSFREE_BOTNET\'falselocation == \'tcp\' ip_reverse + \'.bip.virusfree.cz\'falseSTWT_RBL_VIRUSFREE_IP\0'),(_binary '\0^Hx—Û€²\0',_binary '\0\r!is_empty(ip)\r\'RBL_SPAMCOP\'falselocation == \'tcp\'ip_reverse + \'.bl.spamcop.net\'falseSTWT_RBL_SPAMCOP_IP\0'),(_binary '\0^Hx—\ÛÀ´\0',_binary '\0\r!is_empty(ip)\'RBL_BARRACUDA\'falselocation == \'tcp\'&ip_reverse + \'.b.barracudacentral.org\'falseSTWT_RBL_BARRACUDA_IP\0'),(_binary '\0^Hx—\Û\à¶\0',_binary '\0\"!is_empty(ip) && location == \'tcp\'\'RBL_BLOCKLISTDE\'\r!is_empty(ip)\'RECEIVED_BLOCKLISTDE\'false\0ip_reverse + \'.bl.blocklist.de\'STWT_RBL_BLOCKLISTDE_IP\0'),(_binary '\0^Hx—\Ü ¸\0',_binary '\0octets[0] != 127falseoctets[3] == 0\'RCVD_IN_DNSWL_NONE\'octets[3] == 1\'RCVD_IN_DNSWL_LOW\'octets[3] == 2\'RCVD_IN_DNSWL_MED\'octets[3] == 3\'RCVD_IN_DNSWL_HI\'octets[3] == 255\'DNSWL_BLOCKED\'false\0ip_reverse + \'.list.dnswl.org\'\rSTWT_DNSWL_IP\0'),(_binary '\0^Hx—\Ü@º\0',_binary '\0octets[0] != 127falseoctets[3] == 2\n\'DBL_SPAM\'octets[3] == 4\'DBL_PHISH\'octets[3] == 5\r\'DBL_MALWARE\'octets[3] == 6\'DBL_BOTNET\'octets[3] == 102\'DBL_ABUSE\'octets[3] == 103\'DBL_ABUSE_REDIR\'octets[3] == 104\'DBL_ABUSE_PHISH\'octets[3] == 105\'DBL_ABUSE_MALWARE\'octets[3] == 106\'DBL_ABUSE_BOTNET\'octets[3] == 254\Z\'DBL_BLOCKED_OPENRESOLVER\'octets[3] == 255\r\'DBL_BLOCKED\'false\0value + \'.dbl.spamhaus.org\'STWT_DBL_SPAMHAUS_DOMAIN\0'),(_binary '\0^Hx—Ü€¼\0',_binary '\0octets[3] == 128\'CRACKED_SURBL\'octets[3] == 64\r\'ABUSE_SURBL\'octets[3] == 32\n\'CT_SURBL\'octets[3] == 16\'MW_SURBL_MULTI\'octets[3] == 8\'PH_SURBL_MULTI\'octets[3] == 4\n\'DM_SURBL\'octets[3] == 1\'SURBL_BLOCKED\'false\0\Zvalue + \'.multi.surbl.org\'STWT_SURBL_DOMAIN\0'),(_binary '\0^Hx—Ü ¾\0',_binary '\0octets[3] == 1\'URIBL_BLOCKED\'octets[3] == 2\r\'URIBL_BLACK\'octets[3] == 4\'URIBL_GREY\'octets[3] == 8\'URIBL_RED\'false\0\Zvalue + \'.multi.uribl.com\'STWT_URIBL_DOMAIN\0'),(_binary '\0^Hx—\ÜÀÀ\0',_binary '\0ip == \'127.0.0.2\'\'SEM_URIBL\'false\0%value + \'.uribl.spameatingmonkey.net\'STWT_SEM_URIBL\0'),(_binary '\0^Hx—\İ\0\Â\0',_binary '\0ip == \'127.0.0.2\'\'SEM_URIBL_FRESH15\'false\0\'value + \'.fresh15.spameatingmonkey.net\'STWT_SEM_URIBL_FRESH15\0'),(_binary '\0^Hx—\İ@\Ä\0',_binary '\0octets[0] != 127falseoctets[3] == 0\'DWL_DNSWL_NONE\'octets[3] == 1\'DWL_DNSWL_LOW\'octets[3] == 2\'DWL_DNSWL_MED\'octets[3] == 3\'DWL_DNSWL_HI\'octets[3] == 255\'DWL_DNSWL_BLOCKED\'falselocation == \'dkim_pass\'value + \'.dwl.dnswl.org\'falseSTWT_DWL_DNSWL_DOMAIN\0'),(_binary '\0^Hx—\İ`\Æ\0',_binary '\0octets[0] != 127false4octets[2] == 0 && (octets[3] == 2 || octets[3] == 3)\n\'MSBL_EBL\'4octets[2] == 1 && (octets[3] == 2 || octets[3] == 3)\'MSBL_EBL_GREY\'false\0%hash(email, \'sha1\') + \'.ebl.msbl.org\'STWT_MSBL_EBL_EMAIL\0'),(_binary '\0^Hx—İ€\È\0',_binary '\0octets[0] != 127falseoctets[3] == 8\'SURBL_HASHBL_PHISH\'octets[3] == 16\'SURBL_HASHBL_MALWARE\'octets[3] == 64\'SURBL_HASHBL_ABUSE\'octets[3] == 128\'SURBL_HASHBL_CRACKED\'octets[2] == 1\'SURBL_HASHBL_EMAIL\'false key_exists(\'surbl-hashbl\', host).hash(host + path, \'md5\') + \'.hashbl.surbl.org\'falseSTWT_SURBL_HASHBL_DOMAIN\0'),(_binary '\0cHx—Î€\"\0',_binary '\0\00$MISSING_ESSENTIAL_HEADERS && $SINGLE_SHORT_PART\'SHORT_PART_BAD_HEADERS\'falseSTWT_SHORT_BAD_HEADERS\0\è'),(_binary '\0cHx—\Î\à$\0',_binary '\0\0$FORGED_RECIPIENTS && $MAILLIST\'FORGED_RECIPIENTS_MAILLIST\'falseSTWT_FORGED_RCPT_LIST\0\é'),(_binary '\0cHx—\Ï &\0',_binary '\0\0$FORGED_SENDER && $MAILLIST\'FORGED_SENDER_MAILLIST\'falseSTWT_FORGED_SENDER_LIST\0\ê'),(_binary '\0cHx—\Ï`(\0',_binary '\0\0C$DMARC_POLICY_ALLOW && ($SPF_SOFTFAIL || $SPF_FAIL || $DKIM_REJECT)\"\'DMARC_POLICY_ALLOW_WITH_FAILURES\'false\ZSTWT_DMARC_ALLOW_WITH_FAIL\0\ë'),(_binary '\0cHx—Ï *\0',_binary '\0\0+$DKIM_NA && $SPF_NA && $DMARC_NA && $ARC_NA	\'AUTH_NA\'falseSTWT_AUTH_NA\0\ì'),(_binary '\0cHx—\ÏÀ,\0',_binary '\0\0§!($DKIM_NA && $SPF_NA && $DMARC_NA && $ARC_NA) && ($DKIM_NA || $DKIM_TEMPFAIL || $DKIM_PERMFAIL) && ($SPF_NA || $SPF_DNSFAIL) && $DMARC_NA && ($ARC_NA || $ARC_DNSFAIL)\'AUTH_NA_OR_FAIL\'falseSTWT_AUTH_NA_FAIL\0\í'),(_binary '\0cHx—\Ï\à.\0',_binary '\0\0A($AUTH_NA || $AUTH_NA_OR_FAIL) && ($BOUNCE || $SUBJ_BOUNCE_WORDS)\'BOUNCE_NO_AUTH\'falseSTWT_BOUNCE_NO_AUTH\0\î'),(_binary '\0cHx—\Ğ 0\0',_binary '\0\0\Ø($X_HDR_X_PHP_ORIGINATING_SCRIPT || $HAS_PHPMAILER_SIG) && $HAS_WP_URI && ($PHISHING || $CRACKED_SURBL || $PH_SURBL_MULTI || $DBL_PHISH || $DBL_ABUSE_PHISH || $URIBL_BLACK || $PHISHED_OPENPHISH || $PHISHED_PHISHTANK)\'HACKED_WP_PHISHING\'falseSTWT_HACKED_WP_PHISHING\0\ï'),(_binary '\0cHx—\Ğ@2\0',_binary '\0\0=($X_HDR_X_ORIGINATING_IP || $RCVD_VIA_SMTP_AUTH) && $DCC_BULK\'COMPROMISED_ACCT_BULK\'false\ZSTWT_COMPROMISED_ACCT_BULK\0\ğ'),(_binary '\0cHx—\Ğ`4\0',_binary '\0\0*$DCC_BULK && ($MISSING_TO || $UNDISC_RCPT)\'UNDISC_RCPTS_BULK\'falseSTWT_UNDISC_RCPTS_BULK\0\ñ'),(_binary '\0cHx—Ğ 6\0',_binary '\0\0.$RECEIVED_SPAMHAUS_PBL && !$RCVD_VIA_SMTP_AUTH\'RCVD_UNAUTH_PBL\'falseSTWT_RCVD_UNAUTH_PBL\0\ò'),(_binary '\0cHx—\Ğ\à8\0',_binary '\0\01($DKIM_ALLOW || $ARC_ALLOW) && $RCVD_IN_DNSWL_MED\'RCVD_DKIM_ARC_DNSWL_MED\'falseSTWT_RCVD_DKIM_ARC_DNSWL_MED\0\ó'),(_binary '\0cHx—\Ñ\0:\0',_binary '\0\00($DKIM_ALLOW || $ARC_ALLOW) && $RCVD_IN_DNSWL_HI\'RCVD_DKIM_ARC_DNSWL_HI\'falseSTWT_RCVD_DKIM_ARC_DNSWL_HI\0\ô'),(_binary '\0cHx—\Ñ <\0',_binary '\0\0œ($X_HDR_X_PHP_ORIGINATING_SCRIPT || $HAS_PHPMAILER_SIG || $X_HDR_X_PHP_SCRIPT) && ($SUBJECT_ENDS_QUESTION || $SUBJECT_ENDS_EXCLAIM || $MANY_INVISIBLE_PARTS)\'AUTOGEN_PHP_SPAMMY\'falseSTWT_AUTOGEN_PHP_SPAMMY\0\õ'),(_binary '\0cHx—\Ñ`>\0',_binary '\0\0z($PHISHING || $DBL_PHISH || $PHISHED_OPENPHISH || $PHISHED_PHISHTANK) && ($SUBJECT_ENDS_QUESTION || $SUBJECT_ENDS_EXCLAIM)\'PHISH_EMOTION\'falseSTWT_PHISH_EMOTION\0\ö'),(_binary '\0cHx—Ñ€@\0',_binary '\0\0F$HAS_GUC_PROXY_URI || $URIBL_RED || $DBL_ABUSE_REDIR || $HAS_ONION_URI\'HAS_ANON_DOMAIN\'falseSTWT_HAS_ANON_DOMAIN\0\÷'),(_binary '\0cHx—Ñ B\0',_binary '\0\0G($SPF_FAIL || $SPF_SOFTFAIL) && ($RCVD_COUNT_ZERO || $RCVD_NO_TLS_LAST)\'VIOLATED_DIRECT_SPF\'falseSTWT_VIOLATED_DIRECT_SPF\0ø'),(_binary '\0cHx—\Ñ\àD\0',_binary '\0\0 ($FREEMAIL_FROM || $FREEMAIL_ENV_FROM || $FREEMAIL_REPLY_TO) && ($TO_DN_RECIPIENTS || $UNDISC_RCPT) && ($FROM_NAME_HAS_TITLE || $FREEMAIL_REPLY_TO_NEQ_FROM_DOM)\'FREEMAIL_AFF\'falseSTWT_FREEMAIL_AFF\0ù'),(_binary '\0cHx—\Ò F\0',_binary '\0\0$URL_ONLY && $REDIRECTOR_URL\'REDIRECTOR_URL_ONLY\'falseSTWT_REDIRECTOR_URL_ONLY\0ú'),(_binary '\0cHx—\Ò@H\0',_binary '\0\0s$FAKE_REPLY && $RCVD_VIA_SMTP_AUTH && (!$RECEIVED_SPAMHAUS_PBL || $RECEIVED_SPAMHAUS_XBL || $RECEIVED_SPAMHAUS_SBL) \'THREAD_HIJACKING_FROM_INJECTOR\'falseSTWT_THREAD_HIJACKING\0û'),(_binary '\0cHx—Ò€J\0',_binary '\0\0$X_HDR_DKIM_SIGNATURE\r\'DKIM_SIGNED\'falseSTWT_DKIM_SIGNED\0ü'),(_binary '\0cHx—Ò L\0',_binary '\0\0$X_HDR_ARC_SEAL\'ARC_SIGNED\'falseSTWT_ARC_SIGNED\0ı'),(_binary '\0cHx—\ÒÀN\0',_binary '\0\04$FREEMAIL_REPLY_TO && from.domain != reply_to.domain \'FREEMAIL_REPLY_TO_NEQ_FROM_DOM\'falseSTWT_FREEMAIL_RTO_NEQ_DOM\0ş'),(_binary '\0cHx—\Ò\àP\0',_binary '\0\0M($FREEMAIL_DNT || $DISPOSABLE_DNT) && !($FREEMAIL_FROM || $FREEMAIL_ENV_FROM)\'SUSPICIOUS_MDN\'falseSTWT_SUSPICIOUS_MDN\0ÿ'),(_binary '\0cHx—\Ó R\0',_binary '\0\0™($X_HDR_X_ORIGINATING_IP || $RCVD_VIA_SMTP_AUTH) && ($RECEIVED_SPAMHAUS_PBL || $RECEIVED_SPAMHAUS_XBL || $RECEIVED_SPAMHAUS_SBL || $RECEIVED_BLOCKLISTDE)\'SUSPICIOUS_AUTH_ORIGIN\'falseSTWT_SUSPICIOUS_AUTH_ORIGIN\0€'),(_binary '\0cHx—\Ó`T\0',_binary '\0\0n$SUSPICIOUS_AUTH_ORIGIN && ($RCVD_HELO_USER || $FAKE_REPLY || $HAS_IPFS_GATEWAY_URL || $HTML_SHORT_LINK_IMG_1)\'ABUSE_FROM_INJECTOR\'falseSTWT_SABUSE_FROM_INJECTOR\0'),(_binary '\0cHx—Ó€V\0',_binary '\0\0($MIME_BAD_EXTENSION && $MIME_BAD_UNICODE\'MIME_BAD_EXT_WITH_BAD_UNICODE\'false\"STWT_MIME_BAD_EXT_WITH_BAD_UNICODE\0‚'),(_binary '\0cHx—\ÓÀX\0',_binary '\0\0=contains(subject.words, \'SEO\') || contains(body.words, \'SEO\')\'HAS_SEO_WORD\'falseSTWT_HAS_SEO_WORD\0ƒ'),(_binary '\0cHx—\Ó\àZ\0',_binary '\0\0J$HAS_SEO_WORD && ($RCPT_DOMAIN_IN_BODY || $RCPT_IN_BODY || $FREEMAIL_FROM)\n\'SEO_SPAM\'false\rSTWT_SEO_SPAM\0„'),(_binary '\0cHx—\Ô\0\\\0',_binary '\0\0R$RCVD_COUNT_ZERO && !$RCVD_VIA_SMTP_AUTH && ($X_HDR_USER_AGENT || $X_HDR_X_MAILER)\'DIRECT_TO_MX\'falseSTWT_DIRECT_TO_MX\0…'),(_binary '\0cHx—\Ô ^\0',_binary '\0\0!$HAS_LINK_TO_LARGE_IMGfalse\r$HTML_SHORT_1\'HTML_SHORT_LINK_IMG_1\'\r$HTML_SHORT_2\'HTML_SHORT_LINK_IMG_2\'\r$HTML_SHORT_3\'HTML_SHORT_LINK_IMG_3\'falseSTWT_SHORT_LINK_IMG\0†'),(_binary '\0cHx—\Ô@`\0',_binary '\0\0is_intersect([\'www-data\', \'anonymous\', \'ftp\', \'apache\', \'nobody\', \'guest\', \'nginx\', \'web\', \'www\'], [from.local, env_from.local, reply_to.local])\'FROM_SERVICE_ACCT\'contains(from.local, \'+\')\r\'TAGGED_FROM\'falseSTWT_SVC_OR_TAGGED\0\n'),(_binary '\0cHx—\Ô`b\0',_binary '\0\0Hstarts_with(from.domain, \'www.\') || starts_with(reply_to.domain, \'www.\')\'WWW_DOT_DOMAIN\'falseSTWT_WWW_DOMAIN\0'),(_binary '\0cHx—\ÔÀd\0',_binary '\0\0iis_intersect([\'mr.\', \'mrs.\', \'ms.\', \'dr.\', \'prof.\', \'rev.\', \'hon.\'], split(to_lowercase(from.name), \' \'))\'FROM_NAME_HAS_TITLE\'mis_intersect([\'mr.\', \'mrs.\', \'ms.\', \'dr.\', \'prof.\', \'rev.\', \'hon.\'], split(to_lowercase(reply_to.name), \' \'))\'REPLYTO_EMAIL_HAS_TITLE\'falseSTWT_HAS_TITLE\0'),(_binary '\0cHx—\Ô\àf\0',_binary '\0\0contains(from.name, \'  \')\'FROM_NAME_EXCESS_SPACE\'falseSTWT_FROM_NAME_SPACE\0\r'),(_binary '\0cHx—\Õ\0h\0',_binary '\0\0is_empty(env_from) && ($IS_DSN || $HAS_MESSAGE_PARTS || ($X_HDR_X_MDDSN_MESSAGE && contains_ignore_case(from.name, \'mdaemon\')))\'BOUNCE\'falseSTWT_BOUNCE\0'),(_binary '\0cHx—\Õ j\0',_binary '\0\0B$RCPT_DOMAIN_IN_SUBJECT && ($RCPT_DOMAIN_IN_BODY || $RCPT_IN_BODY)\'RCPT_DOMAIN_IN_MESSAGE\'falseSTWT_RCPT_DOMAIN_IN_MESSAGE\0'),(_binary '\0cHx—\Õ@l\0',_binary '\0\0A$DMARC_POLICY_ALLOW && key_exists(\'trusted-domains\', from.domain)\'TRUSTED_DOMAIN\'falseSTWT_TRUSTED_DOMAIN\0'),(_binary '\0cHx—\Õ`n\0',_binary '\0\0*key_exists(\'blocked-domains\', from.domain)\'BLOCKED_DOMAIN\'falseSTWT_BLOCKED_DOMAIN\0'),(_binary '\0cHx—Õ p\0',_binary '\0Ocontains([\'to\', \'cc\', \'bcc\'], name_lower) && contains(raw_lower, \'undisclosed\')\r\'UNDISC_RCPT\'falseSTWT_R_UNDISC_RCPT\0<'),(_binary '\0cHx—\ÕÀr\0',_binary '\0?name_lower == \'x-authenticated-sender\' && contains(value, \': \')\n\'HAS_X_AS\'false\rSTWT_HAS_X_AS\0='),(_binary '\0cHx—\Ö\0t\0',_binary '\0Vname_lower == \'x-get-message-sender-via\' && contains(value_lower, \'authenticated_id:\')\'HAS_X_GMSV\'falseSTWT_HAS_X_GMSV\0>'),(_binary '\0cHx—\Ö@v\0',_binary '\0]contains([\'x-source\', \'x-source-args\', \'x-source-dir\'], name_lower) && contains(value, \'../\')\'HIDDEN_SOURCE_OBJ\'falseSTWT_HIDDEN_SOURCE_OBJ\0?'),(_binary '\0cHx—\Ö`x\0',_binary '\0econtains(value_lower, \'eval()\') && contains([\'x-php-originating-script\', \'x-php-script\'], name_lower)\'X_PHP_EVAL\'falseSTWT_X_PHP_EVAL\0@'),(_binary '\0cHx—Ö€z\0',_binary '\0\\contains(value, \'../\') && contains([\'x-php-originating-script\', \'x-php-script\'], name_lower)\'HIDDEN_SOURCE_OBJ\'falseSTWT_HIDDEN_SOURCE_PHP\0A'),(_binary '\0cHx—Ö |\0',_binary '\0wcontains([\'x-ui-filterresults\', \'x-ui-out-filterresults\', \'x-source-dir\'], name_lower) && contains(value_lower, \'junk\')\'UNITEDINTERNET_SPAM\'falseSTWT_UNITEDINTERNET_SPAM\0B'),(_binary '\0cHx—\ÖÀ~\0',_binary '\0¤contains([\'x-spam\', \'x-spam-flag\', \'x-spam-status\'], name_lower) && (contains(value_lower, \'yes\') || contains(value_lower, \'true\') || contains(value_lower, \'spam\'))\'SPAM_FLAG\'falseSTWT_SPAM_FLAG\0C'),(_binary '\0cHx—\× €\0',_binary '\0Gname_lower == \'x-klms-antispam-status\' && contains(value_lower, \'spam\')\'KLMS_SPAM\'falseSTWT_KLMS_SPAM\0D'),(_binary '\0cHx—\×@‚\0',_binary '\0.name_lower == \'x-mailer\' && name != \'X-Mailer\'	\'XM_CASE\'falseSTWT_XM_CASE\0E'),(_binary '\0cHx—\×`„\0',_binary '\0b(name_lower == \'user_agent\' || name_lower == \'x-mailer\') && !is_empty(value) && !has_digits(value)\'XM_UA_NO_VERSION\'falseSTWT_XM_UA_NO_VERSION\0F'),(_binary '\0cHx—×€†\0',_binary '\0>name_lower == \'x-mailer\' && contains(value_lower, \'phpmailer\')\'HAS_PHPMAILER_SIG\'falseSTWT_HAS_PHPMAILER_SIG\0G'),(_binary '\0cHx—\×Àˆ\0',_binary '\0Rcontains([\'to\', \'cc\', \'bcc\'], location) && contains_ignore_case(name, \'recipient\')\'TO_DN_RECIPIENTS\'falseSTWT_TO_DN_RECIPIENTS\0<'),(_binary '\0cHx—\×\àŠ\0',_binary '\0mcontains([\'to\', \'cc\', \'bcc\'], location) && local == \'info\' && from.local == \'info\' && $X_HDR_LIST_UNSUBSCRIBE\'INFO_TO_INFO_LU\'falseSTWT_INFO_INFO_LU\0='),(_binary '\0cHx—\Ø\0Œ\0',_binary '\0?contains([\'to\', \'cc\', \'bcc\'], location) && contains(local, \'+\')\r\'TAGGED_RCPT\'falseSTWT_TAGGED_RCPT\0>'),(_binary '\0cHx—\Ø@\0',_binary '\0`!contains([\'env_from\', \'from\', \'reply_to\', \'to\', \'cc\', \'bcc\', \'dnt\'], location) || is_empty(sld)false\'key_exists(\'stwt_free_domains\', domain)$\'FREEMAIL_\' + to_uppercase(location)-key_exists(\'stwt_disposable_domains\', domain)&\'DISPOSABLE_\' + to_uppercase(location)falseSTWT_FREE_OR_DISP\0?'),(_binary '\0cHx—Ø€\0',_binary '\0\0(contains(subject, \'delivery\') &&              (contains(subject, \'failed\') ||               contains(subject, \'report\') ||               contains(subject, \'status\') ||               contains(subject, \'warning\'))) ||          (contains(subject, \'failure\') &&              (contains(subject, \'delivery\') ||               contains(subject, \'notice\') ||               contains(subject, \'mail\') )) ||          (contains(subject, \'delivered\') &&             (contains(subject, \'couldn\\\'t be\') ||               contains(subject, \'could not be\') ||               contains(subject, \'hasn\\\'t been\') ||               contains(subject, \'has not been\'))) ||          contains(subject, \'returned mail\') ||          contains(subject, \'undeliverable\') ||           contains(subject, \'undelivered\')\'SUBJ_BOUNCE_WORDS\'falseSTWT_SUBJ_BOUNCE_WORDS\0'),(_binary '\0cHx—Ø ’\0',_binary '\0\0…!$BOUNCE && is_empty(env_from) && $SUBJ_BOUNCE_WORDS && (contains(from.local, \'postmaster\') || contains(from.local, \'mailer-daemon\'))\'BOUNCE\'falseSTWT_BOUNCE_SUBJECT\0'),(_binary '\0cHx—\ØÀ”\0',_binary '\0\0ends_with(trim(subject), \'!\')\'SUBJECT_ENDS_EXCLAIM\'ends_with(trim(subject), \'?\')\'SUBJECT_ENDS_QUESTION\'contains(subject, \'!\')\'SUBJECT_HAS_EXCLAIM\'contains(subject, \'?\')\'SUBJECT_HAS_QUESTION\'falseSTWT_SUBJECT_HAS_SYMBOLS\0'),(_binary '\0cHx—\Ù\0–\0',_binary '\0\0len(subject) > 200\'LONG_SUBJ\'falseSTWT_LONG_SUBJ\0'),(_binary '\0cHx—\Ù ˜\0',_binary '\0\0zcontains_ignore_case([\'re\', \'aw\', \'antw\', \'sv\'], split_once(subject, \':\')[0]) && !$X_HDR_IN_REPLY_TO && !$X_HDR_REFERENCES\'FAKE_REPLY\'falseSTWT_FAKE_REPLY\0'),(_binary '\0cHx—Ù€š\0',_binary '\0!key_exists(\'stwt_openphish\', url)\'PHISHED_OPENPHISH\'falseSTWT_PHISHED_OPEN\02'),(_binary '\0cHx—Ù œ\0',_binary '\0!key_exists(\'stwt_phishtank\', url)\'PHISHED_PHISHTANK\'falseSTWT_PHISHED_TANK\03'),(_binary '\0cHx—\ÙÀ\0',_binary '\0Nends_with(host, \'googleusercontent.com\') && starts_with(path_query, \'/proxy/\')\'HAS_GUC_PROXY_URI\'1ends_with(host, \'firebasestorage.googleapis.com\')\'HAS_GOOGLE_FIREBASE_URL\';starts_with(sld, \'google.\') && contains(path_query, \'url?\')\'HAS_GOOGLE_REDIR\'falseSTWT_HAS_GOOGLE_URL\04'),(_binary '\0cHx—\Ù\à \0',_binary '\0Y(contains(host, \'ipfs.\') || contains(path_query, \'/ipfs\')) && contains(path_query, \'/qm\')\'HAS_IPFS_GATEWAY_URL\'ends_with(host, \'.onion\')\'HAS_ONION_URI\'falseSTWT_IPFS_OR_ONION\05'),(_binary '\0cHx—\Ú\0¢\0',_binary '\0Estarts_with(path, \'/wp-content\') || starts_with(path, \'/wp-includes\')\'WP_COMPROMISED\'starts_with(path, \'/wp-\')\'HAS_WP_URI\'falseSTWT_WP_COMPROMISED\06'),(_binary '\0cHx—\Ú@¤\0',_binary '\0lcontains(path_query, \'/../\') && !contains(path_query, \'/well-known\') && !contains(path_query, \'/well_known\')\'URI_HIDDEN_PATH\'falseSTWT_URI_HIDDEN_PATH\07'),(_binary '\0eHx—İ \Ê\0',_binary '\0\0ABUSE_FROM_INJECTOR€€€€€€€€@'),(_binary '\0eHx—\İ\à\Ì\0',_binary '\0\0ABUSE_SURBL€€€€€€€Š@'),(_binary '\0eHx—\Ş\0\Î\0',_binary '\0\0	ARC_ALLOW\0'),(_binary '\0eHx—\Ş \Ğ\0',_binary '\0\0ARC_DNSFAIL\0'),(_binary '\0eHx—\Ş`\Ò\0',_binary '\0\0ARC_INVALID€€€€€€€\ğ?'),(_binary '\0eHx—Ş \Ô\0',_binary '\0\0ARC_NA\0'),(_binary '\0eHx—\ŞÀ\Ö\0',_binary '\0\0\nARC_REJECT€€€€€€€ø?'),(_binary '\0eHx—\Ş\à\Ø\0',_binary '\0\0\nARC_SIGNED\0'),(_binary '\0eHx—\ß\0\Ú\0',_binary '\0\0AUTH_NA€€€€€€€ø?'),(_binary '\0eHx—\ß \Ü\0',_binary '\0\0AUTH_NA_OR_FAIL€€€€€€€ø?'),(_binary '\0eHx—\ß@\Ş\0',_binary '\0\0AUTOGEN_PHP_SPAMMY€€€€€€€ø?'),(_binary '\0eHx—\ß`\à\0',_binary '\0\0BAD_CTE_7BIT€€€€€€€†@'),(_binary '\0eHx—ß \â\0',_binary '\0BLOCKED_DOMAIN'),(_binary '\0eHx—\ßÀ\ä\0',_binary '\0\0\rBODY_URI_ONLY€€€€€€€€@'),(_binary '\0eHx—\ß\à\æ\0',_binary '\0\0BOGUS_ENCRYPTED_AND_TEXT€€€€€€€’@'),(_binary '\0eHx—\à \è\0',_binary '\0\0BOUNCEš³\æÌ™³\æÜ¿'),(_binary '\0eHx—\à@\ê\0',_binary '\0\0BOUNCE_NO_AUTH€€€€€€€ø?'),(_binary '\0eHx—\à€\ì\0',_binary '\0\0BROKEN_CONTENT_TYPE€€€€€€€ü?'),(_binary '\0eHx—\à \î\0',_binary '\0\0COMPLETELY_EMPTY€€€€€€€@'),(_binary '\0eHx—\àÀ\ğ\0',_binary '\0\0COMPROMISED_ACCT_BULK€€€€€€€„@'),(_binary '\0eHx—\á\0\ò\0',_binary '\0\0\rCRACKED_SURBL€€€€€€€Š@'),(_binary '\0eHx—\á@\ô\0',_binary '\0\0CTE_CASE€€€€€€€\ğ?'),(_binary '\0eHx—\á`\ö\0',_binary '\0\0CTYPE_MISSING_DISPOSITION€€€€€€€ˆ@'),(_binary '\0eHx—\á€ø\0',_binary '\0\0CTYPE_MIXED_BOGUS€€€€€€€ø?'),(_binary '\0eHx—\á ú\0',_binary '\0\0\rCT_EXTRA_SEMI€€€€€€€ø?'),(_binary '\0eHx—\áÀü\0',_binary '\0\0\rDATA_URI_OBFU€€€€€€€€@'),(_binary '\0eHx—\â\0ş\0',_binary '\0\0DATE_IN_FUTURE€€€€€€€ˆ@'),(_binary '\0eHx—\âA\0\0',_binary '\0\0DATE_IN_PAST€€€€€€€ø?'),(_binary '\0eHx—\âa\0',_binary '\0\0	DBL_ABUSE€€€€€€€Š@'),(_binary '\0eHx—\â\0',_binary '\0\0DBL_ABUSE_BOTNET€€€€€€€@'),(_binary '\0eHx—\âÁ\0',_binary '\0\0DBL_ABUSE_MALWARE€€€€€€€@'),(_binary '\0eHx—\â\á\0',_binary '\0\0DBL_ABUSE_PHISH€€€€€€€@'),(_binary '\0eHx—\ã\n\0',_binary '\0\0DBL_ABUSE_REDIR€€€€€€€Š@'),(_binary '\0eHx—\ã!\0',_binary '\0\0DBL_BLOCKED\0'),(_binary '\0eHx—\ãa\0',_binary '\0\0DBL_BLOCKED_OPENRESOLVER\0'),(_binary '\0eHx—\ã\0',_binary '\0\0\nDBL_BOTNET€€€€€€€@'),(_binary '\0eHx—\ãÁ\0',_binary '\0\0DBL_MALWARE€€€€€€€@'),(_binary '\0eHx—\ã\á\0',_binary '\0\0	DBL_PHISH€€€€€€€@'),(_binary '\0eHx—\ä\0',_binary '\0\0DBL_SPAM€€€€€€€@'),(_binary '\0eHx—\êA\0',_binary '\0\0DCC_BULK€€€€€€€„@'),(_binary '\0eHx—\ê\Z\0',_binary '\0\0DIRECT_TO_MX€€€€€€€€@'),(_binary '\0eHx—\ê¡\0',_binary '\0\0\rDISPOSABLE_CC\0'),(_binary '\0eHx—\êÁ\0',_binary '\0\0DISPOSABLE_DNT€€€€€€€\ğ?'),(_binary '\0eHx—\ê\á \0',_binary '\0\0DISPOSABLE_ENV_FROM\0'),(_binary '\0eHx—\ë!\"\0',_binary '\0\0DISPOSABLE_FROM\0'),(_binary '\0eHx—\ëA$\0',_binary '\0\0DISPOSABLE_REPLYTO\0'),(_binary '\0eHx—\ëa&\0',_binary '\0\0\rDISPOSABLE_TO\0'),(_binary '\0eHx—\ë(\0',_binary '\0\0\nDKIM_ALLOWš³\æÌ™³\æ\ä¿'),(_binary '\0eHx—\ë¡*\0',_binary '\0\0DKIM_NA\0'),(_binary '\0eHx—\ëÁ,\0',_binary '\0\0\rDKIM_PERMFAIL\0'),(_binary '\0eHx—\ë\á.\0',_binary '\0\0DKIM_REJECT€€€€€€€ø?'),(_binary '\0eHx—\ì0\0',_binary '\0\0DKIM_SIGNED\0'),(_binary '\0eHx—\ìA2\0',_binary '\0\0\rDKIM_TEMPFAIL\0'),(_binary '\0eHx—\ìa4\0',_binary '\0\0DMARC_BAD_POLICY€€€€€€€\ğ?'),(_binary '\0eHx—\ì6\0',_binary '\0\0\rDMARC_DNSFAIL\0'),(_binary '\0eHx—\ì¡8\0',_binary '\0\0DMARC_NA€€€€€€€ø?'),(_binary '\0eHx—\ìÁ:\0',_binary '\0\0DMARC_POLICY_ALLOW€€€€€€€\ğ¿'),(_binary '\0eHx—\ìÁ<\0',_binary '\0\0 DMARC_POLICY_ALLOW_WITH_FAILURES\0'),(_binary '\0eHx—\ì\á>\0',_binary '\0\0DMARC_POLICY_QUARANTINE€€€€€€€ü?'),(_binary '\0eHx—\í!@\0',_binary '\0\0DMARC_POLICY_REJECT€€€€€€€ˆ@'),(_binary '\0eHx—\íAB\0',_binary '\0\0DMARC_POLICY_SOFTFAILš³\æÌ™³\æ\Ü?'),(_binary '\0eHx—\íaD\0',_binary '\0\0\rDNSWL_BLOCKED\0'),(_binary '\0eHx—\í¡F\0',_binary '\0\0DWL_DNSWL_BLOCKED\0'),(_binary '\0eHx—\íÁH\0',_binary '\0\0DWL_DNSWL_HI€€€€€€€†À'),(_binary '\0eHx—\í\áJ\0',_binary '\0\0\rDWL_DNSWL_LOW€€€€€€€ø¿'),(_binary '\0eHx—\í\áL\0',_binary '\0\0\rDWL_DNSWL_MED€€€€€€€€À'),(_binary '\0eHx—\îAN\0',_binary '\0\0DWL_DNSWL_NONE\0'),(_binary '\0eHx—\îaP\0',_binary '\0\0\rEMPTY_SUBJECT€€€€€€€ø?'),(_binary '\0eHx—\îR\0',_binary '\0\0\rENCRYPTED_PGP€€€€€€€\ğ¿'),(_binary '\0eHx—\î¡T\0',_binary '\0\0ENCRYPTED_SMIME€€€€€€€\ğ¿'),(_binary '\0eHx—\î¡V\0',_binary '\0\0ENV_FROM_INVALID€€€€€€€€@'),(_binary '\0eHx—\îÁX\0',_binary '\0\0EXT_CSS€€€€€€€ø?'),(_binary '\0eHx—\î\áZ\0',_binary '\0\0\nFAKE_REPLY€€€€€€€ø?'),(_binary '\0eHx—\ïA\\\0',_binary '\0\0FORGED_RCVD_TRAIL€€€€€€€ø?'),(_binary '\0eHx—\ïa^\0',_binary '\0\0FORGED_RECIPIENTS€€€€€€€€@'),(_binary '\0eHx—\ï¡`\0',_binary '\0\0\ZFORGED_RECIPIENTS_MAILLIST\0'),(_binary '\0eHx—\ïÁb\0',_binary '\0\0\rFORGED_SENDER³\æÌ™³\æ\Ì\é?'),(_binary '\0eHx—\ğd\0',_binary '\0\0FORGED_SENDER_MAILLIST\0'),(_binary '\0eHx—\ğ!f\0',_binary '\0\0FREEMAIL_AFF€€€€€€€ˆ@'),(_binary '\0eHx—\ğAh\0',_binary '\0\0FREEMAIL_CC\0'),(_binary '\0eHx—\ğaj\0',_binary '\0\0FREEMAIL_DNT\0'),(_binary '\0eHx—\ğl\0',_binary '\0\0FREEMAIL_ENV_FROM\0'),(_binary '\0eHx—\ğ¡n\0',_binary '\0\0\rFREEMAIL_FROM\0'),(_binary '\0eHx—\ğÁp\0',_binary '\0\0FREEMAIL_REPLY_TO\0'),(_binary '\0eHx—\ğ\ár\0',_binary '\0\0FREEMAIL_REPLY_TO_NEQ_FROM_DOM€€€€€€€„@'),(_binary '\0eHx—\ñt\0',_binary '\0\0FREEMAIL_TO\0'),(_binary '\0eHx—\ñ!v\0',_binary '\0\0FROMHOST_NORES_A_OR_MX€€€€€€€ü?'),(_binary '\0eHx—\ñ!x\0',_binary '\0\0FROM_BOUNCE\0'),(_binary '\0eHx—\ñAz\0',_binary '\0\0FROM_DN_EQ_ADDR€€€€€€€ø?'),(_binary '\0eHx—\ñ|\0',_binary '\0\0FROM_EQ_ENV_FROM\0'),(_binary '\0eHx—\ñÁ~\0',_binary '\0\0FROM_EXCESS_BASE64€€€€€€€ü?'),(_binary '\0eHx—\ñ\á€\0',_binary '\0\0FROM_EXCESS_QP³\æÌ™³\æ\Ìù?'),(_binary '\0eHx—\ò‚\0',_binary '\0\0FROM_HAS_DN\0'),(_binary '\0eHx—\ò!„\0',_binary '\0\0FROM_INVALID€€€€€€€€@'),(_binary '\0eHx—\òA†\0',_binary '\0\0FROM_NAME_EXCESS_SPACE€€€€€€€ø?'),(_binary '\0eHx—\òaˆ\0',_binary '\0\0FROM_NAME_HAS_TITLE€€€€€€€ø?'),(_binary '\0eHx—\òŠ\0',_binary '\0\0FROM_NEEDS_ENCODING€€€€€€€ø?'),(_binary '\0eHx—\òÁŒ\0',_binary '\0\0FROM_NEQ_DISPLAY_NAME€€€€€€€ˆ@'),(_binary '\0eHx—\ò\á\0',_binary '\0\0FROM_NEQ_ENV_FROM\0'),(_binary '\0eHx—\ó\0',_binary '\0\0\nFROM_NO_DN\0'),(_binary '\0eHx—\óA’\0',_binary '\0\0FROM_SERVICE_ACCT€€€€€€€ø?'),(_binary '\0eHx—\óa”\0',_binary '\0\0\nGTUBE_TEST€€€€€€\Ğ\Ç@'),(_binary '\0eHx—\ó–\0',_binary '\0\0HACKED_WP_PHISHING€€€€€€€‰@'),(_binary '\0eHx—\ó¡˜\0',_binary '\0\0HAS_ANON_DOMAINš³\æÌ™³\æ\Ü?'),(_binary '\0eHx—\óÁš\0',_binary '\0\0HAS_ATTACHMENT\0'),(_binary '\0eHx—\ó\áœ\0',_binary '\0\0HAS_DATA_URI\0'),(_binary '\0eHx—\ô\0',_binary '\0\0HAS_GOOGLE_FIREBASE_URL€€€€€€€€@'),(_binary '\0eHx—\ôa \0',_binary '\0\0HAS_GOOGLE_REDIR€€€€€€€ø?'),(_binary '\0eHx—\ô¢\0',_binary '\0\0HAS_GUC_PROXY_URI€€€€€€€ø?'),(_binary '\0eHx—\ô¡¤\0',_binary '\0\0HAS_IPFS_GATEWAY_URL€€€€€€€Œ@'),(_binary '\0eHx—\ôÁ¦\0',_binary '\0\0HAS_LIST_UNSUBû¨¸½”ÜÂ¿'),(_binary '\0eHx—\õ¨\0',_binary '\0\0\rHAS_ONION_URI\0'),(_binary '\0eHx—\õ!ª\0',_binary '\0\0HAS_PHPMAILER_SIG\0'),(_binary '\0eHx—\õA¬\0',_binary '\0\0HAS_REPLYTO\0'),(_binary '\0eHx—\õa®\0',_binary '\0\0HAS_SEO_WORD\0'),(_binary '\0eHx—\õ°\0',_binary '\0\0\nHAS_WP_URI\0'),(_binary '\0eHx—\õ¡²\0',_binary '\0\0HAS_X_AS\0'),(_binary '\0eHx—\õ\á´\0',_binary '\0\0\nHAS_X_GMSV\0'),(_binary '\0eHx—\ö¶\0',_binary '\0\0HAS_X_PRIO_FIVE\0'),(_binary '\0eHx—\ö!¸\0',_binary '\0\0HAS_X_PRIO_ONE\0'),(_binary '\0eHx—\öAº\0',_binary '\0\0HAS_X_PRIO_THREE\0'),(_binary '\0eHx—\öa¼\0',_binary '\0\0HAS_X_PRIO_TWO\0'),(_binary '\0eHx—\ö¡¾\0',_binary '\0\0HAS_X_PRIO_ZERO\0'),(_binary '\0eHx—\öÁÀ\0',_binary '\0\0HEADER_EMPTY_DELIMITER€€€€€€€ø?'),(_binary '\0eHx—\ö\á\Â\0',_binary '\0\0HEADER_FORGED_MDN€€€€€€€€@'),(_binary '\0eHx—\÷A\Ä\0',_binary '\0\0HEADER_RCONFIRM_MISMATCH€€€€€€€€@'),(_binary '\0eHx—\÷a\Æ\0',_binary '\0\0HELO_BAREIP€€€€€€€„@'),(_binary '\0eHx—\÷\È\0',_binary '\0\0HELO_IPREV_MISMATCH€€€€€€€ø?'),(_binary '\0eHx—\÷¡\Ê\0',_binary '\0\0	HELO_IP_A€€€€€€€ø?'),(_binary '\0eHx—\÷¡\Ì\0',_binary '\0\0HELO_NORES_A_OR_MX³\æÌ™³\æ\Ì\é?'),(_binary '\0eHx—\÷Á\Î\0',_binary '\0\0\rHELO_NOT_FQDN€€€€€€€€@'),(_binary '\0eHx—\÷\á\Ğ\0',_binary '\0\0HIDDEN_SOURCE_OBJ€€€€€€€€@'),(_binary '\0eHx—ø\Ò\0',_binary '\0\0\rHOMOGRAPH_URL€€€€€€€Š@'),(_binary '\0eHx—ø!\Ô\0',_binary '\0\0HTML_META_REFRESH_URL€€€€€€€Š@'),(_binary '\0eHx—øa\Ö\0',_binary '\0\0HTML_SHORT_LINK_IMG_1€€€€€€€€@'),(_binary '\0eHx—ø\Ø\0',_binary '\0\0HTML_SHORT_LINK_IMG_2€€€€€€€ø?'),(_binary '\0eHx—ø¡\Ú\0',_binary '\0\0HTML_SHORT_LINK_IMG_3€€€€€€€\ğ?'),(_binary '\0eHx—øÁ\Ü\0',_binary '\0\0HTML_TEXT_IMG_RATIO€€€€€€€ø?'),(_binary '\0eHx—ø\á\Ş\0',_binary '\0\0HTML_UNBALANCED_TAG€€€€€€€\ğ?'),(_binary '\0eHx—ù\à\0',_binary '\0\0\rHTTP_TO_HTTPS€€€€€€€\ğ?'),(_binary '\0eHx—ù!\â\0',_binary '\0\0\nHTTP_TO_IP€€€€€€€ø?'),(_binary '\0eHx—ùa\ä\0',_binary '\0\0INFO_TO_INFO_LU€€€€€€€€@'),(_binary '\0eHx—ùa\æ\0',_binary '\0\0INVALID_DATE€€€€€€€ü?'),(_binary '\0eHx—ù\è\0',_binary '\0\0INVALID_FROM_8BIT€€€€€€€Œ@'),(_binary '\0eHx—ù¡\ê\0',_binary '\0\0\rINVALID_MSGID³\æÌ™³\æ\Ìı?'),(_binary '\0eHx—ùÁ\ì\0',_binary '\0\0	KLMS_SPAM€€€€€€€Š@'),(_binary '\0eHx—ù\á\î\0',_binary '\0\0LLM_COMMERCIAL_HIGH€€€€€€€„@'),(_binary '\0eHx—ú\ğ\0',_binary '\0\0LLM_COMMERCIAL_LOW€€€€€€€\ğ?'),(_binary '\0eHx—úA\ò\0',_binary '\0\0LLM_COMMERCIAL_MEDIUM€€€€€€€€@'),(_binary '\0eHx—ú\ô\0',_binary '\0\0LLM_HARMFUL_HIGH€€€€€€€„@'),(_binary '\0eHx—ú¡\ö\0',_binary '\0\0LLM_HARMFUL_LOW€€€€€€€\ğ?'),(_binary '\0eHx—ú\áø\0',_binary '\0\0LLM_HARMFUL_MEDIUM€€€€€€€€@'),(_binary '\0eHx—û!ú\0',_binary '\0\0LLM_LEGITIMATE_HIGH€€€€€€€„À'),(_binary '\0eHx—ûAü\0',_binary '\0\0LLM_LEGITIMATE_LOW€€€€€€€\ğ¿'),(_binary '\0eHx—ûaş\0',_binary '\0\0LLM_LEGITIMATE_MEDIUM€€€€€€€€À'),(_binary '\0eHx—û\Â\0\0',_binary '\0\0LLM_UNSOLICITED_HIGH€€€€€€€„@'),(_binary '\0eHx—û\â\0',_binary '\0\0LLM_UNSOLICITED_LOW€€€€€€€\ğ?'),(_binary '\0eHx—û\â\0',_binary '\0\0LLM_UNSOLICITED_MEDIUM€€€€€€€€@'),(_binary '\0eHx—ü\0',_binary '\0\0	LONG_SUBJ€€€€€€€„@'),(_binary '\0eHx—ü\"\0',_binary '\0\0MAILLISTš³\æÌ™³\æ\ä¿'),(_binary '\0eHx—üB\n\0',_binary '\0\0MANY_INVISIBLE_PARTS€€€€€€€ø?'),(_binary '\0eHx—üb\0',_binary '\0\0MID_BARE_IP€€€€€€€€@'),(_binary '\0eHx—ü‚\0',_binary '\0\0MID_CONTAINS_FROM€€€€€€€ø?'),(_binary '\0eHx—ü\â\0',_binary '\0\0MID_CONTAINS_TO€€€€€€€ø?'),(_binary '\0eHx—ü\â\0',_binary '\0\0MID_MISSING_BRACKETS€€€€€€€ø?'),(_binary '\0eHx—ı\0',_binary '\0\0MID_RHS_IP_LITERAL€€€€€€€ø?'),(_binary '\0eHx—ı\"\0',_binary '\0\0MID_RHS_MATCH_FROM€€€€€€€ø?'),(_binary '\0eHx—ıB\0',_binary '\0\0MID_RHS_MATCH_FROMTLD\0'),(_binary '\0eHx—ıb\Z\0',_binary '\0\0MID_RHS_MATCH_TO€€€€€€€ø?'),(_binary '\0eHx—ı‚\0',_binary '\0\0MID_RHS_NOT_FQDN€€€€€€€\ğ?'),(_binary '\0eHx—ı¢\0',_binary '\0\0MID_RHS_WWW€€€€€€€\ğ?'),(_binary '\0eHx—ı\Â \0',_binary '\0\0MIME_ARCHIVE_IN_ARCHIVE€€€€€€€Š@'),(_binary '\0eHx—ı\â\"\0',_binary '\0\0MIME_BAD€€€€€€€ø?'),(_binary '\0eHx—ş$\0',_binary '\0\0MIME_BAD_ATTACHMENT€€€€€€€ˆ@'),(_binary '\0eHx—şB&\0',_binary '\0\0MIME_BAD_EXTENSION€€€€€€€€@'),(_binary '\0eHx—şb(\0',_binary '\0\0MIME_BAD_EXT_WITH_BAD_UNICODE€€€€€€€@'),(_binary '\0eHx—ş‚*\0',_binary '\0\0MIME_BAD_UNICODE€€€€€€€@'),(_binary '\0eHx—ş¢,\0',_binary '\0\0MIME_BASE64_TEXTš³\æÌ™³\æ\Ü?'),(_binary '\0eHx—ş\Â.\0',_binary '\0\0MIME_BASE64_TEXT_BOGUS€€€€€€€ø?'),(_binary '\0eHx—ş\â0\0',_binary '\0\0MIME_DOUBLE_BAD_EXTENSION€€€€€€€€@'),(_binary '\0eHx—ÿ2\0',_binary '\0\0	MIME_GOODš³\æÌ™³\æÜ¿'),(_binary '\0eHx—ÿ\"4\0',_binary '\0\0MIME_HEADER_CTYPE_ONLY€€€€€€€€@'),(_binary '\0eHx—ÿb6\0',_binary '\0\0MIME_HTML_ONLYš³\æÌ™³\æ\ä?'),(_binary '\0eHx—ÿ‚8\0',_binary '\0\0MIME_MA_MISSING_HTML€€€€€€€ø?'),(_binary '\0eHx—ÿ¢:\0',_binary '\0\0MIME_MA_MISSING_TEXT€€€€€€€€@'),(_binary '\0eHx—ÿ\Â<\0',_binary '\0\0MISSING_CHARSET€€€€€€€\ğ?'),(_binary '\0eHx—ÿ\â>\0',_binary '\0\0MISSING_DATE€€€€€€€ø?'),(_binary '\0eHx˜\0@\0',_binary '\0\0MISSING_ESSENTIAL_HEADERS€€€€€€€@'),(_binary '\0eHx˜\0\"B\0',_binary '\0\0MISSING_FROM€€€€€€€€@'),(_binary '\0eHx˜\0BD\0',_binary '\0\0MISSING_MID€€€€€€€‚@'),(_binary '\0eHx˜\0‚F\0',_binary '\0\0MISSING_MIME_VERSION€€€€€€€€@'),(_binary '\0eHx˜\0¢H\0',_binary '\0\0MISSING_SUBJECT€€€€€€€€@'),(_binary '\0eHx˜\0\ÂJ\0',_binary '\0\0\nMISSING_TO€€€€€€€€@'),(_binary '\0eHx˜\0\âL\0',_binary '\0\0\rMIXED_CHARSET€€€€€€€€@'),(_binary '\0eHx˜N\0',_binary '\0\0MIXED_CHARSET_URL€€€€€€€@'),(_binary '\0eHx˜\"P\0',_binary '\0\0MSBL_EBL€€€€€€€@'),(_binary '\0eHx˜BR\0',_binary '\0\0\rMSBL_EBL_GREY€€€€€€€\ğ?'),(_binary '\0eHx˜bT\0',_binary '\0\0\rMULTIPLE_FROM€€€€€€€@'),(_binary '\0eHx˜¢V\0',_binary '\0\0MULTIPLE_UNIQUE_HEADERS€€€€€€€@'),(_binary '\0eHx˜\ÂX\0',_binary '\0\0MV_CASE€€€€€€€\ğ?'),(_binary '\0eHx˜\âZ\0',_binary '\0\0MW_SURBL_MULTI€€€€€€€@'),(_binary '\0eHx˜\\\0',_binary '\0\0NO_SPACE_IN_FROM€€€€€€€ø?'),(_binary '\0eHx˜\"^\0',_binary '\0\0PARTS_DIFFER€€€€€€€ø?'),(_binary '\0eHx˜\"`\0',_binary '\0\0PHISHED_OPENPHISH€€€€€€€@'),(_binary '\0eHx˜Bb\0',_binary '\0\0PHISHED_PHISHTANK€€€€€€€@'),(_binary '\0eHx˜bd\0',_binary '\0\0PHISHING€€€€€€€ˆ@'),(_binary '\0eHx˜¢f\0',_binary '\0\0\rPHISH_EMOTION€€€€€€€ø?'),(_binary '\0eHx˜¢h\0',_binary '\0\0PH_SURBL_MULTI€€€€€€€@'),(_binary '\0eHx˜\âj\0',_binary '\0\0PRECEDENCE_BULK\0'),(_binary '\0eHx˜\"l\0',_binary '\0\0PREVIOUSLY_DELIVERED\0'),(_binary '\0eHx˜bn\0',_binary '\0\0\rPROB_HAM_HIGH€€€€€€€À'),(_binary '\0eHx˜‚p\0',_binary '\0\0PROB_HAM_LOW€€€€€€€€À'),(_binary '\0eHx˜¢r\0',_binary '\0\0PROB_HAM_MEDIUM€€€€€€€ŒÀ'),(_binary '\0eHx˜\Ât\0',_binary '\0\0PROB_SPAM_HIGH€€€€€€€@'),(_binary '\0eHx˜v\0',_binary '\0\0\rPROB_SPAM_LOW€€€€€€€€@'),(_binary '\0eHx˜\"x\0',_binary '\0\0PROB_SPAM_MEDIUM€€€€€€€Œ@'),(_binary '\0eHx˜Bz\0',_binary '\0\0PROB_SPAM_UNCERTAIN\0'),(_binary '\0eHx˜b|\0',_binary '\0\0PYZOR€€€€€€€†@'),(_binary '\0eHx˜‚~\0',_binary '\0\0\rRBL_BARRACUDA€€€€€€€ˆ@'),(_binary '\0eHx˜¢€\0',_binary '\0\0RBL_BLOCKLISTDE€€€€€€€ˆ@'),(_binary '\0eHx˜Â‚\0',_binary '\0\0RBL_MAILSPIKE_BAD€€€€€€€ø?'),(_binary '\0eHx˜„\0',_binary '\0\0RBL_MAILSPIKE_VERYBAD€€€€€€€ü?'),(_binary '\0eHx˜\"†\0',_binary '\0\0RBL_MAILSPIKE_WORST€€€€€€€€@'),(_binary '\0eHx˜Bˆ\0',_binary '\0\0RBL_SEM€€€€€€€ø?'),(_binary '\0eHx˜bŠ\0',_binary '\0\0RBL_SEM_IPV6€€€€€€€ø?'),(_binary '\0eHx˜‚Œ\0',_binary '\0\0RBL_SENDERSCORE_BLOCKED\0'),(_binary '\0eHx˜¢\0',_binary '\0\0RBL_SENDERSCORE_BOT€€€€€€€€@'),(_binary '\0eHx˜Â\0',_binary '\0\0RBL_SENDERSCORE_NA\0'),(_binary '\0eHx˜\â’\0',_binary '\0\0RBL_SENDERSCORE_NA_BOT€€€€€€€ø?'),(_binary '\0eHx˜”\0',_binary '\0\0RBL_SENDERSCORE_PRST€€€€€€€€@'),(_binary '\0eHx˜B–\0',_binary '\0\0RBL_SENDERSCORE_PRST_BOT€€€€€€€„@'),(_binary '\0eHx˜b˜\0',_binary '\0\0RBL_SENDERSCORE_PRST_NA€€€€€€€€@'),(_binary '\0eHx˜‚š\0',_binary '\0\0RBL_SENDERSCORE_PRST_NA_BOT€€€€€€€„@'),(_binary '\0eHx˜¢œ\0',_binary '\0\0RBL_SENDERSCORE_REPUT_0€€€€€€€ˆ@'),(_binary '\0eHx˜Â\0',_binary '\0\0RBL_SENDERSCORE_REPUT_1€€€€€€€†@'),(_binary '\0eHx˜\â \0',_binary '\0\0RBL_SENDERSCORE_REPUT_2€€€€€€€„@'),(_binary '\0eHx˜\"¢\0',_binary '\0\0RBL_SENDERSCORE_REPUT_3€€€€€€€‚@'),(_binary '\0eHx˜B¤\0',_binary '\0\0RBL_SENDERSCORE_REPUT_4€€€€€€€€@'),(_binary '\0eHx˜b¦\0',_binary '\0\0RBL_SENDERSCORE_REPUT_5€€€€€€€ü?'),(_binary '\0eHx˜‚¨\0',_binary '\0\0RBL_SENDERSCORE_REPUT_6€€€€€€€ø?'),(_binary '\0eHx˜¢ª\0',_binary '\0\0RBL_SENDERSCORE_REPUT_7€€€€€€€\ğ?'),(_binary '\0eHx˜Â¬\0',_binary '\0\0RBL_SENDERSCORE_REPUT_8\0'),(_binary '\0eHx˜\â®\0',_binary '\0\0RBL_SENDERSCORE_REPUT_9€€€€€€€ø¿'),(_binary '\0eHx˜°\0',_binary '\0\0RBL_SENDERSCORE_REPUT_BLOCKED\0'),(_binary '\0eHx˜\"²\0',_binary '\0\0RBL_SENDERSCORE_REPUT_UNKNOWN\0'),(_binary '\0eHx˜B´\0',_binary '\0\0RBL_SENDERSCORE_SCORE€€€€€€€€@'),(_binary '\0eHx˜‚¶\0',_binary '\0\0RBL_SENDERSCORE_SCORE_NA€€€€€€€€@'),(_binary '\0eHx˜‚¸\0',_binary '\0\0\ZRBL_SENDERSCORE_SCORE_PRST€€€€€€€ˆ@'),(_binary '\0eHx˜¢º\0',_binary '\0\0RBL_SENDERSCORE_SCORE_PRST_NA€€€€€€€ˆ@'),(_binary '\0eHx˜Â¼\0',_binary '\0\0 RBL_SENDERSCORE_SCORE_SUS_ATT_NA€€€€€€€„@'),(_binary '\0eHx˜\â¾\0',_binary '\0\0RBL_SENDERSCORE_SUS_ATT€€€€€€€ø?'),(_binary '\0eHx˜	À\0',_binary '\0\0\ZRBL_SENDERSCORE_SUS_ATT_NA€€€€€€€ø?'),(_binary '\0eHx˜	\"\Â\0',_binary '\0\0RBL_SENDERSCORE_SUS_ATT_NA_BOT€€€€€€€ü?'),(_binary '\0eHx˜	b\Ä\0',_binary '\0\0RBL_SENDERSCORE_SUS_ATT_PRST_NA€€€€€€€„@'),(_binary '\0eHx˜	‚\Æ\0',_binary '\0\0#RBL_SENDERSCORE_SUS_ATT_PRST_NA_BOT€€€€€€€†@'),(_binary '\0eHx˜	¢\È\0',_binary '\0\0RBL_SPAMCOP€€€€€€€ˆ@'),(_binary '\0eHx˜	\Â\Ê\0',_binary '\0\0RBL_SPAMHAUS_BLOCKED\0'),(_binary '\0eHx˜	\â\Ì\0',_binary '\0\0!RBL_SPAMHAUS_BLOCKED_OPENRESOLVER\0'),(_binary '\0eHx˜\n\Î\0',_binary '\0\0RBL_SPAMHAUS_CSS€€€€€€€€@'),(_binary '\0eHx˜\n\"\Ğ\0',_binary '\0\0RBL_SPAMHAUS_DROP€€€€€€€@'),(_binary '\0eHx˜\nB\Ò\0',_binary '\0\0RBL_SPAMHAUS_PBL€€€€€€€€@'),(_binary '\0eHx˜\nb\Ô\0',_binary '\0\0RBL_SPAMHAUS_SBL€€€€€€€ˆ@'),(_binary '\0eHx˜\n‚\Ö\0',_binary '\0\0RBL_SPAMHAUS_XBL€€€€€€€ˆ@'),(_binary '\0eHx˜\n\Â\Ø\0',_binary '\0\0RBL_VIRUSFREE_BOTNET€€€€€€€€@'),(_binary '\0eHx˜\n\â\Ú\0',_binary '\0\0RCPT_BOUNCEMOREONE€€€€€€€ü?'),(_binary '\0eHx˜\Ü\0',_binary '\0\0RCPT_COUNT_FIVE\0'),(_binary '\0eHx˜\"\Ş\0',_binary '\0\0RCPT_COUNT_GT_50€€€€€€€ø?'),(_binary '\0eHx˜B\à\0',_binary '\0\0RCPT_COUNT_ONE\0'),(_binary '\0eHx˜b\â\0',_binary '\0\0RCPT_COUNT_SEVEN\0'),(_binary '\0eHx˜‚\ä\0',_binary '\0\0RCPT_COUNT_THREE\0'),(_binary '\0eHx˜\Â\æ\0',_binary '\0\0RCPT_COUNT_TWELVE\0'),(_binary '\0eHx˜\â\è\0',_binary '\0\0RCPT_COUNT_TWO\0'),(_binary '\0eHx˜\ê\0',_binary '\0\0RCPT_COUNT_ZERO\0'),(_binary '\0eHx˜\"\ì\0',_binary '\0\0RCPT_DOMAIN_IN_MESSAGE€€€€€€€€@'),(_binary '\0eHx˜B\î\0',_binary '\0\0RCPT_DOMAIN_IN_SUBJECT€€€€€€€€@'),(_binary '\0eHx˜‚\ğ\0',_binary '\0\0RCPT_IN_SUBJECT€€€€€€€„@'),(_binary '\0eHx˜\Â\ò\0',_binary '\0\0RCPT_LOCAL_IN_SUBJECT€€€€€€€€@'),(_binary '\0eHx˜\Â\ô\0',_binary '\0\0RCVD_COUNT_FIVE\0'),(_binary '\0eHx˜\â\ö\0',_binary '\0\0RCVD_COUNT_ONE\0'),(_binary '\0eHx˜\r\"ø\0',_binary '\0\0RCVD_COUNT_SEVEN\0'),(_binary '\0eHx˜\rBú\0',_binary '\0\0RCVD_COUNT_THREE\0'),(_binary '\0eHx˜\r‚ü\0',_binary '\0\0RCVD_COUNT_TWELVE\0'),(_binary '\0eHx˜\r¢ş\0',_binary '\0\0RCVD_COUNT_TWO\0'),(_binary '\0eHx˜\r\Ã\0\0',_binary '\0\0RCVD_COUNT_ZEROš³\æÌ™³\æ\Ü?'),(_binary '\0eHx˜\r\Ã\0',_binary '\0\0RCVD_DKIM_ARC_DNSWL_HI€€€€€€€ø¿'),(_binary '\0eHx˜#\0',_binary '\0\0RCVD_DKIM_ARC_DNSWL_MED€€€€€€€\ğ¿'),(_binary '\0eHx˜C\0',_binary '\0\0RCVD_DOUBLE_IP_SPAM€€€€€€€€@'),(_binary '\0eHx˜c\0',_binary '\0\0RCVD_HELO_USER€€€€€€€„@'),(_binary '\0eHx˜c\n\0',_binary '\0\0RCVD_ILLEGAL_CHARS€€€€€€€ˆ@'),(_binary '\0eHx˜ƒ\0',_binary '\0\0RCVD_IN_DNSWL_HI€€€€€€€\ğ¿'),(_binary '\0eHx˜£\0',_binary '\0\0RCVD_IN_DNSWL_LOWš³\æÌ™³\æÜ¿'),(_binary '\0eHx˜\Ã\0',_binary '\0\0RCVD_IN_DNSWL_MEDš³\æÌ™³\æ\ä¿'),(_binary '\0eHx˜#\0',_binary '\0\0RCVD_IN_DNSWL_NONE\0'),(_binary '\0eHx˜C\0',_binary '\0\0RCVD_NO_TLS_LASTš³\æÌ™³\æ\Ü?'),(_binary '\0eHx˜ƒ\0',_binary '\0\0RCVD_TLS_ALL\0'),(_binary '\0eHx˜ƒ\0',_binary '\0\0\rRCVD_TLS_LAST\0'),(_binary '\0eHx˜£\Z\0',_binary '\0\0RCVD_UNAUTH_PBL€€€€€€€€@'),(_binary '\0eHx˜\Ã\0',_binary '\0\0RCVD_VIA_SMTP_AUTH\0'),(_binary '\0eHx˜\0',_binary '\0\0RDNS_DNSFAIL\0'),(_binary '\0eHx˜# \0',_binary '\0\0	RDNS_NONE€€€€€€€€@'),(_binary '\0eHx˜C\"\0',_binary '\0\0RECEIVED_BLOCKLISTDE€€€€€€€„@'),(_binary '\0eHx˜c$\0',_binary '\0\0RECEIVED_SPAMHAUS_BLOCKED\0'),(_binary '\0eHx˜ƒ&\0',_binary '\0\0&RECEIVED_SPAMHAUS_BLOCKED_OPENRESOLVER\0'),(_binary '\0eHx˜\Ã(\0',_binary '\0\0RECEIVED_SPAMHAUS_CSS€€€€€€€ø?'),(_binary '\0eHx˜\ã*\0',_binary '\0\0RECEIVED_SPAMHAUS_PBL\0'),(_binary '\0eHx˜,\0',_binary '\0\0RECEIVED_SPAMHAUS_SBL€€€€€€€„@'),(_binary '\0eHx˜#.\0',_binary '\0\0RECEIVED_SPAMHAUS_XBL€€€€€€€ø?'),(_binary '\0eHx˜C0\0',_binary '\0\0REDIRECTOR_URL\0'),(_binary '\0eHx˜ƒ2\0',_binary '\0\0REDIRECTOR_URL_ONLY€€€€€€€ø?'),(_binary '\0eHx˜£4\0',_binary '\0\0REPLYTO_ADDR_EQ_FROM\0'),(_binary '\0eHx˜\Ã6\0',_binary '\0\0REPLYTO_DN_EQ_FROM_DN\0'),(_binary '\0eHx˜\ã8\0',_binary '\0\0REPLYTO_DOM_EQ_FROM_DOM\0'),(_binary '\0eHx˜:\0',_binary '\0\0REPLYTO_DOM_NEQ_FROM_DOM\0'),(_binary '\0eHx˜C<\0',_binary '\0\0REPLYTO_EMAIL_HAS_TITLE€€€€€€€€@'),(_binary '\0eHx˜c>\0',_binary '\0\0REPLYTO_EQ_FROM\0'),(_binary '\0eHx˜ƒ@\0',_binary '\0\0REPLYTO_EQ_TO_ADDR€€€€€€€Š@'),(_binary '\0eHx˜£B\0',_binary '\0\0REPLYTO_EXCESS_BASE64€€€€€€€ü?'),(_binary '\0eHx˜\ãD\0',_binary '\0\0REPLYTO_EXCESS_QP³\æÌ™³\æ\Ìù?'),(_binary '\0eHx˜\ãF\0',_binary '\0\0REPLYTO_UNPARSABLE€€€€€€€ø?'),(_binary '\0eHx˜H\0',_binary '\0\0RWL_MAILSPIKE_EXCELLENTš³\æÌ™³\æ\ì¿'),(_binary '\0eHx˜#J\0',_binary '\0\0RWL_MAILSPIKE_GOODš³\æÌ™³\æÜ¿'),(_binary '\0eHx˜CL\0',_binary '\0\0RWL_MAILSPIKE_NEUTRAL\0'),(_binary '\0eHx˜£N\0',_binary '\0\0RWL_MAILSPIKE_POSSIBLE\0'),(_binary '\0eHx˜\ÃP\0',_binary '\0\0RWL_MAILSPIKE_VERYGOODš³\æÌ™³\æ\ä¿'),(_binary '\0eHx˜\ÃR\0',_binary '\0\0	SEM_URIBL€€€€€€€†@'),(_binary '\0eHx˜\ãT\0',_binary '\0\0SEM_URIBL_FRESH15€€€€€€€„@'),(_binary '\0eHx˜V\0',_binary '\0\0SEO_SPAM€€€€€€€Š@'),(_binary '\0eHx˜#X\0',_binary '\0\0SHORT_PART_BAD_HEADERS€€€€€€€@'),(_binary '\0eHx˜CZ\0',_binary '\0\0\nSIGNED_PGP€€€€€€€€À'),(_binary '\0eHx˜£\\\0',_binary '\0\0SIGNED_SMIME€€€€€€€€À'),(_binary '\0eHx˜\ã^\0',_binary '\0\0SINGLE_SHORT_PART\0'),(_binary '\0eHx˜`\0',_binary '\0\0\rSORTED_RECIPS€€€€€€€†@'),(_binary '\0eHx˜#b\0',_binary '\0\0	SPAM_FLAG€€€€€€€Š@'),(_binary '\0eHx˜Cd\0',_binary '\0\0	SPAM_TRAP€€€€€€€—@'),(_binary '\0eHx˜cf\0',_binary '\0\0	SPF_ALLOWš³\æÌ™³\æ\ä¿'),(_binary '\0eHx˜ch\0',_binary '\0\0SPF_DNSFAIL\0'),(_binary '\0eHx˜£j\0',_binary '\0\0SPF_FAIL€€€€€€€ø?'),(_binary '\0eHx˜\Ãl\0',_binary '\0\0SPF_NA\0'),(_binary '\0eHx˜n\0',_binary '\0\0SPF_NEUTRAL\0'),(_binary '\0i\0\0CL²M\Í',_binary '\0\ntest.local\0\0€@\0\0\n\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'),(_binary '\0qHx’V€\0\0',_binary '\0\0/var/lib/stalwart/logsstalwart\0\0\0\0'),(_binary '\0r\0\0CL²M\Í',_binary '\0');
/*!40000 ALTER TABLE `s` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `s_cal`
--

DROP TABLE IF EXISTS `s_cal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `s_cal` (
  `accid` int NOT NULL,
  `docid` int NOT NULL,
  `titl` text COLLATE utf8mb4_unicode_ci,
  `dscd` text COLLATE utf8mb4_unicode_ci,
  `locn` text COLLATE utf8mb4_unicode_ci,
  `ownr` text COLLATE utf8mb4_unicode_ci,
  `atnd` text COLLATE utf8mb4_unicode_ci,
  `strt` bigint NOT NULL,
  `uid` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`accid`,`docid`),
  KEY `idx_s_cal_strt` (`strt`),
  FULLTEXT KEY `fts_s_cal_titl` (`titl`),
  FULLTEXT KEY `fts_s_cal_dscd` (`dscd`),
  FULLTEXT KEY `fts_s_cal_locn` (`locn`),
  FULLTEXT KEY `fts_s_cal_ownr` (`ownr`),
  FULLTEXT KEY `fts_s_cal_atnd` (`atnd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `s_cal`
--

LOCK TABLES `s_cal` WRITE;
/*!40000 ALTER TABLE `s_cal` DISABLE KEYS */;
/*!40000 ALTER TABLE `s_cal` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `s_card`
--

DROP TABLE IF EXISTS `s_card`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `s_card` (
  `accid` int NOT NULL,
  `docid` int NOT NULL,
  `mmbr` text COLLATE utf8mb4_unicode_ci,
  `kind` text COLLATE utf8mb4_unicode_ci,
  `name` text COLLATE utf8mb4_unicode_ci,
  `nick` text COLLATE utf8mb4_unicode_ci,
  `orgn` text COLLATE utf8mb4_unicode_ci,
  `eml` text COLLATE utf8mb4_unicode_ci,
  `phon` text COLLATE utf8mb4_unicode_ci,
  `olsv` text COLLATE utf8mb4_unicode_ci,
  `addr` text COLLATE utf8mb4_unicode_ci,
  `note` text COLLATE utf8mb4_unicode_ci,
  `uid` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`accid`,`docid`),
  FULLTEXT KEY `fts_s_card_mmbr` (`mmbr`),
  FULLTEXT KEY `fts_s_card_name` (`name`),
  FULLTEXT KEY `fts_s_card_nick` (`nick`),
  FULLTEXT KEY `fts_s_card_orgn` (`orgn`),
  FULLTEXT KEY `fts_s_card_eml` (`eml`),
  FULLTEXT KEY `fts_s_card_phon` (`phon`),
  FULLTEXT KEY `fts_s_card_olsv` (`olsv`),
  FULLTEXT KEY `fts_s_card_addr` (`addr`),
  FULLTEXT KEY `fts_s_card_note` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `s_card`
--

LOCK TABLES `s_card` WRITE;
/*!40000 ALTER TABLE `s_card` DISABLE KEYS */;
/*!40000 ALTER TABLE `s_card` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `s_email`
--

DROP TABLE IF EXISTS `s_email`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `s_email` (
  `accid` int NOT NULL,
  `docid` int NOT NULL,
  `fadr` text COLLATE utf8mb4_unicode_ci,
  `tadr` text COLLATE utf8mb4_unicode_ci,
  `cc` text COLLATE utf8mb4_unicode_ci,
  `bcc` text COLLATE utf8mb4_unicode_ci,
  `subj` text COLLATE utf8mb4_unicode_ci,
  `body` mediumtext COLLATE utf8mb4_unicode_ci,
  `atta` mediumtext COLLATE utf8mb4_unicode_ci,
  `rcvd` bigint DEFAULT NULL,
  `sent` bigint DEFAULT NULL,
  `size` int DEFAULT NULL,
  `hatt` tinyint(1) DEFAULT NULL,
  `hdrs` json DEFAULT NULL,
  PRIMARY KEY (`accid`,`docid`),
  KEY `idx_s_email_rcvd` (`rcvd`),
  KEY `idx_s_email_sent` (`sent`),
  KEY `idx_s_email_size` (`size`),
  KEY `idx_s_email_hatt` (`hatt`),
  FULLTEXT KEY `fts_s_email_fadr` (`fadr`),
  FULLTEXT KEY `fts_s_email_tadr` (`tadr`),
  FULLTEXT KEY `fts_s_email_cc` (`cc`),
  FULLTEXT KEY `fts_s_email_bcc` (`bcc`),
  FULLTEXT KEY `fts_s_email_subj` (`subj`),
  FULLTEXT KEY `fts_s_email_body` (`body`),
  FULLTEXT KEY `fts_s_email_atta` (`atta`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `s_email`
--

LOCK TABLES `s_email` WRITE;
/*!40000 ALTER TABLE `s_email` DISABLE KEYS */;
/*!40000 ALTER TABLE `s_email` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `s_trace`
--

DROP TABLE IF EXISTS `s_trace`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `s_trace` (
  `id` bigint NOT NULL,
  `etyp` bigint DEFAULT NULL,
  `qid` bigint DEFAULT NULL,
  `kwds` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`id`),
  KEY `idx_s_trace_etyp` (`etyp`),
  KEY `idx_s_trace_qid` (`qid`),
  FULLTEXT KEY `fts_s_trace_kwds` (`kwds`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `s_trace`
--

LOCK TABLES `s_trace` WRITE;
/*!40000 ALTER TABLE `s_trace` DISABLE KEYS */;
/*!40000 ALTER TABLE `s_trace` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `t`
--

DROP TABLE IF EXISTS `t`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `t` (
  `k` tinyblob NOT NULL,
  `v` longblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `t`
--

LOCK TABLES `t` WRITE;
/*!40000 ALTER TABLE `t` DISABLE KEYS */;
/*!40000 ALTER TABLE `t` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `u`
--

DROP TABLE IF EXISTS `u`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `u` (
  `k` tinyblob NOT NULL,
  `v` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `u`
--

LOCK TABLES `u` WRITE;
/*!40000 ALTER TABLE `u` DISABLE KEYS */;
/*!40000 ALTER TABLE `u` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `w`
--

DROP TABLE IF EXISTS `w`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `w` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `w`
--

LOCK TABLES `w` WRITE;
/*!40000 ALTER TABLE `w` DISABLE KEYS */;
/*!40000 ALTER TABLE `w` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `x`
--

DROP TABLE IF EXISTS `x`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `x` (
  `k` tinyblob NOT NULL,
  `v` mediumblob NOT NULL,
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `x`
--

LOCK TABLES `x` WRITE;
/*!40000 ALTER TABLE `x` DISABLE KEYS */;
/*!40000 ALTER TABLE `x` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `y`
--

DROP TABLE IF EXISTS `y`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `y` (
  `k` tinyblob NOT NULL,
  `v` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`k`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `y`
--

LOCK TABLES `y` WRITE;
/*!40000 ALTER TABLE `y` DISABLE KEYS */;
INSERT INTO `y` VALUES (_binary '\0\0\0\0\0\0\0Ä‰V',5);
/*!40000 ALTER TABLE `y` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'stalwart'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-05-22 10:30:57
