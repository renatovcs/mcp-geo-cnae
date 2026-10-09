"""
Testes automatizados para validação do servidor MCP e consultas GraphRAG com dados reais da Receita Federal.
"""

import unittest
from typing import Any, Dict, List

from server import (
    buscar_coocorrencia_cnaes_global,
    buscar_coocorrencia_cnaes_municipio,
    buscar_malha_municipal_vizinha,
    buscar_perfil_economico_municipio,
)


class TestMCPGeoCNAEReal(unittest.TestCase):
    def test_buscar_coocorrencia_global_software(self) -> None:
        cnae = "6201-5/01"
        resultado: List[Dict[str, Any]] = buscar_coocorrencia_cnaes_global(cnae, limite=5)
        self.assertIsInstance(resultado, list)
        self.assertGreater(len(resultado), 0)

        # Valida que as frequências estão em ordem decrescente
        frequencias = [item["frequencia"] for item in resultado]
        self.assertEqual(frequencias, sorted(frequencias, reverse=True))

        # O CNAE com maior coocorrência nacional com desenvolvimento de software é 6204-0/00 (Consultoria em TI)
        self.assertEqual(resultado[0]["codigo"], "6204-0/00")
        self.assertGreater(resultado[0]["frequencia"], 30000)

    def test_buscar_coocorrencia_municipio_sao_paulo(self) -> None:
        cnae_sem_ponto = "6201501"
        ibge_sp = "3550308"
        resultado: List[Dict[str, Any]] = buscar_coocorrencia_cnaes_municipio(
            cnae_sem_ponto, ibge_sp, limite=5
        )
        self.assertIsInstance(resultado, list)
        self.assertGreater(len(resultado), 0)

        for item in resultado:
            self.assertEqual(item["codigo_ibge"], ibge_sp)

        # Frequência decrescente
        frequencias = [item["frequencia"] for item in resultado]
        self.assertEqual(frequencias, sorted(frequencias, reverse=True))

    def test_buscar_malha_municipal_curitiba(self) -> None:
        # Busca por nome do município de forma flexível
        resultado: Dict[str, Any] = buscar_malha_municipal_vizinha("Curitiba")

        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["codigo_ibge"], "4106902")
        self.assertEqual(resultado["nome"], "Curitiba")
        self.assertEqual(resultado["estado"]["uf"], "PR")

        vizinhos = resultado["municipios_vizinhos"]
        self.assertGreater(len(vizinhos), 0)

        # Pinhais e São José dos Pinhais devem estar entre os vizinhos
        nomes_vizinhos = [v["nome"] for v in vizinhos]
        self.assertIn("Pinhais", nomes_vizinhos)
        self.assertIn("São José dos Pinhais", nomes_vizinhos)

        # Valida distância geodésica positiva
        for v in vizinhos:
            self.assertGreater(v["distancia_km"], 0.0)

    def test_buscar_perfil_economico_municipio(self) -> None:
        resultado: Dict[str, Any] = buscar_perfil_economico_municipio("3550308", top_cnaes=5)
        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["nome"], "São Paulo")
        self.assertGreater(resultado["total_empresas_ativas_mapeadas"], 1000000)
        self.assertEqual(len(resultado["principais_cnaes"]), 5)

    def test_municipio_inexistente(self) -> None:
        ibge_fantasma = "9999999"
        resultado: Dict[str, Any] = buscar_malha_municipal_vizinha(ibge_fantasma)
        self.assertFalse(resultado["encontrado"])
        self.assertEqual(resultado["municipios_vizinhos"], [])


if __name__ == "__main__":
    unittest.main()
