import unittest
from unittest.mock import MagicMock

from neo4j.exceptions import ServiceUnavailable

from server import MemgraphClient, normalizar_cnae, normalizar_municipio


class TestUnitHelpers(unittest.TestCase):
    def test_normalizar_cnae_com_e_sem_mascara(self) -> None:
        self.assertEqual(normalizar_cnae("6201-5/01"), ("6201-5/01", "6201501"))
        self.assertEqual(normalizar_cnae("6201501"), ("6201-5/01", "6201501"))
        self.assertEqual(normalizar_cnae("  6201.5-01 "), ("6201-5/01", "6201501"))

    def test_normalizar_cnae_invalido_preserva_texto(self) -> None:
        self.assertEqual(normalizar_cnae("123"), ("123", "123"))

    def test_normalizar_municipio_remove_acentos_e_hifens(self) -> None:
        self.assertEqual(normalizar_municipio("São Paulo"), "SAO PAULO")
        self.assertEqual(normalizar_municipio("  Florianópolis  "), "FLORIANOPOLIS")
        self.assertEqual(normalizar_municipio("Embu-Guaçu"), "EMBU GUACU")
        self.assertEqual(normalizar_municipio(""), "")

    def test_memgraph_client_reseta_driver_em_service_unavailable(self) -> None:
        client = MemgraphClient(uri="bolt://localhost:7687")
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value = mock_session
        client._driver = mock_driver

        with self.assertRaises(ServiceUnavailable):
            with client.session():
                raise ServiceUnavailable("Conexao perdida")

        mock_session.close.assert_called_once()
        mock_driver.close.assert_called_once()
        self.assertIsNone(client._driver)


if __name__ == "__main__":
    unittest.main()
