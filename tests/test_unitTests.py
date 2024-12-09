import unittest
import IrisDataMasker

class MaskersTest(unittest.TestCase):

  def testRedact(self):
    self.assertEqual("******",IrisDataMasker.redact("hello!","*"))

  def testPartial(self):
    self.assertEqual("123****890",IrisDataMasker.partialRedact("1234567890",3,3,"*"))

  def testRegexStar(self):
    self.assertEqual("**/**/****",IrisDataMasker.regex("01/01/2000","[0-9]","*"))

  def testRegexNumber(self):
    self.assertEqual("00/00/0000",IrisDataMasker.regex("01/01/2000","[0-9]","0"))

  def testRegexEmail(self):
    self.assertEqual("t******@mail.com",IrisDataMasker.regex("testingaccount@mail.com","(?<=^.).+(?=.*@)","******"))