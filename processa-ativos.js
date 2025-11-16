require('dotenv').config();
const mysql = require('mysql2/promise');

// Busca regra específica para o ativo pelo código
async function getRegraPorCodigo(conn, codigo) {
  const [rows] = await conn.execute(
    'SELECT limite_alta, limite_queda, invertido, ativo FROM regras_ativos WHERE codigo = ?',
    [codigo]
  );
  return rows.length > 0 ? rows[0] : null;
}

async function processarAtivos(cotacoes) {
  const conn = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME
  });

  const timestamp = new Date();

  for (const c of cotacoes) {
    const variacao = c.variacao || 0;
    let sinal = 'Neutro';

    // Busca regra personalizada
    const regra = await getRegraPorCodigo(conn, c.codigo);

    // Se regra existe e ativo está desativado, pula
    if (regra && regra.ativo === 0) {
      console.log(`Ativo ${c.nome} (${c.codigo}) desativado. Ignorando...`);
      continue;
    }

    // Aplica regra personalizada ou padrão
    if (regra) {
      if (!regra.invertido) {
        if (variacao < regra.limite_queda) sinal = 'Alta';
        else if (variacao > regra.limite_alta) sinal = 'Queda';
      } else {
        if (variacao < regra.limite_queda) sinal = 'Queda';
        else if (variacao > regra.limite_alta) sinal = 'Alta';
      }
    } else {
      // Regra padrão se não houver no banco
      if (variacao >= -0.3 && variacao <= 0.3) sinal = 'Neutro';
      else if (variacao > 0.3) sinal = 'Queda';
      else sinal = 'Alta';
    }

    // Define tipo (risco ou segurança)
    const tipo = definirTipoAtivo(c.nome);

    // Define tipo_contagem:
    // - Ativos normais = 'simple'
    // - Ativos compostos (identificados pelo flag no objeto) = 'composto'
    const tipoContagem = c.tipo_contagem || 'simple';

    // Insere no banco
    await conn.execute(
      'INSERT INTO ativos (tipo, nome, sinal, variacao, timestamp, tipo_contagem) VALUES (?, ?, ?, ?, ?, ?)',
      [tipo, c.nome, sinal, variacao, timestamp, tipoContagem]
    );
  }

  await conn.end();
}

// Função auxiliar para definir tipo do ativo
function definirTipoAtivo(nome) {
  return nome.includes('USD') ? 'seguranca' : 'risco';
}

module.exports = { processarAtivos };