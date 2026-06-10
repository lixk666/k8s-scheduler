import unittest

from load_aware_scheduler.quantity import parse_cpu_cores, parse_memory_bytes


class QuantityTest(unittest.TestCase):
    def test_cpu(self):
        self.assertEqual(parse_cpu_cores("1"), 1.0)
        self.assertEqual(parse_cpu_cores("500m"), 0.5)
        self.assertEqual(parse_cpu_cores("250000000n"), 0.25)

    def test_memory(self):
        self.assertEqual(parse_memory_bytes("1Ki"), 1024)
        self.assertEqual(parse_memory_bytes("2Mi"), 2 * 1024 * 1024)
        self.assertEqual(parse_memory_bytes("1G"), 1000 * 1000 * 1000)


if __name__ == "__main__":
    unittest.main()
