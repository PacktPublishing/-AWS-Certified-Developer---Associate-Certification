import unittest
from CFexample import validateTemplate,templates,REGION,createClient
client = createClient(REGION)


class ValidateTemplates(unittest.TestCase):
    def test_included_templates(self):
        for i in range(0,len(templates)):
            responsecode=validateTemplate(client,templates[i])
            self.assertEqual(200,responsecode)


if __name__ == '__main__':
    unittest.main()
