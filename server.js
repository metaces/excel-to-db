require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');

const app = express();
app.use(cors());
app.use(express.json());


const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME
};

// Busca regra específica para o ativo pelo código
async function getRegraPorCodigo(conn, codigo) {
  const [rows] = await conn.execute(
    'SELECT * FROM regras_ativos WHERE codigo = ?',
    [codigo]
  );
  return rows.length > 0 ? rows[0] : null;
}

app.get('/ativos/mapa', async (req, res) => {
  const { dia, horaInicial } = req.query; // Ex: dia=2025-11-09, horaInicial=08:00
  try {
    const conn = await mysql.createConnection(dbConfig);

    let query = `
      SELECT nome, tipo, sinal, COUNT(*) as qtd
      FROM ativos
      WHERE DATE(timestamp) = ?
    `;
    const params = [dia];

    if (horaInicial) {
      query += ` AND TIME(timestamp) >= ?`;
      params.push(horaInicial);
    }

    query += ` GROUP BY nome, tipo, sinal`;

    const [rows] = await conn.execute(query, params);
    await conn.end();
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erro ao buscar mapa de calor dos ativos' });
  }
});

// Histórico completo
app.get('/grafico', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute('SELECT * FROM grafico ORDER BY timestamp DESC');
        await conn.end();
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Erro ao buscar histórico do gráfico' });
    }
});

// Últimos 20 registros
app.get('/grafico/latest', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute('SELECT * FROM grafico ORDER BY timestamp DESC LIMIT 20');
        await conn.end();
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Erro ao buscar últimos registros do gráfico' });
    }
});

// Todos os sinais do dia
app.get('/ativos', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute(`
            SELECT * FROM ativos WHERE DATE(timestamp) = CURDATE() ORDER BY timestamp DESC
        `);
        await conn.end();
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Erro ao buscar sinais dos ativos' });
    }
});

app.post('/estrangeiro', async (req, res) => {
  const { compra, venda } = req.body;
  if (!compra || !venda) {
    return res.status(400).json({ error: 'Campos compra e venda são obrigatórios' });
  }

  const diferenca = parseFloat(compra) - parseFloat(venda); // CORREÇÃO
  const timestamp = new Date();
  const codigo = 'ESTRANGEIRO';

  try {
    const conn = await mysql.createConnection(dbConfig);

    // Buscar último registro anterior
    const [ultimos] = await conn.execute(
      'SELECT diferenca FROM estrangeiro ORDER BY timestamp DESC LIMIT 1'
    );
    const diferencaAnterior = ultimos.length > 0 ? parseFloat(ultimos[0].diferenca) : 0;
    
    // Calcular acum (diferença atual - diferença anterior)
    const acum = diferenca - diferencaAnterior // CORREÇÃO
    
    // Inserir na tabela estrangeiro
    await conn.execute(
      'INSERT INTO estrangeiro (compra, venda, diferenca, acum, timestamp, codigo) VALUES (?, ?, ?, ?, ?, ?)',
      [compra, venda, diferenca, acum, timestamp, codigo]
    );

    // Busca regra personalizada
    const regra = await getRegraPorCodigo(conn, codigo);

    // Se regra existe e ativo está desativado, pula
    if (regra && regra.ativo === 0) {
      console.log(`Ativo ${regra.nome} (${regra.codigo}) desativado. Ignorando...`);
      return;
    }

    let sinal = 'Neutro';
    // Aplica regra personalizada ou padrão
    if (regra) {
      if (!regra.invertido) {
        if (acum < regra.limite_queda) sinal = 'Alta';
        else if (acum > regra.limite_alta) sinal = 'Queda';
      } else {
        if (acum < regra.limite_queda) sinal = 'Queda';
        else if (acum > regra.limite_alta) sinal = 'Alta';
      }
    } else {
      // Regra padrão se não houver no banco
      if (acum >= -0.3 && acum <= 0.3) sinal = 'Neutro';
      else if (acum > 0.3) sinal = 'Queda';
      else sinal = 'Alta';
    }

    // Define tipo (risco ou segurança)
    const tipo = definirTipoAtivo(regra.nome);

    // Define tipo_contagem:
    // - Ativos normais = 'simple'
    // - Ativos compostos (identificados pelo flag no objeto) = 'composto'
    const tipoContagem = 'simple';

    // Insere no banco
    await conn.execute(
      'INSERT INTO ativos (tipo, nome, sinal, variacao, timestamp, tipo_contagem) VALUES (?, ?, ?, ?, ?, ?)',
      [tipo, regra.nome, sinal, acum, timestamp, tipoContagem]
    );

    await conn.end();
    res.json({ message: 'Dados inseridos com sucesso', compra, venda, diferenca, acum, sinal });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erro ao inserir dados estrangeiro' });
  }
});

app.get('/estrangeiro', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute('SELECT * FROM estrangeiro ORDER BY timestamp DESC');
        await conn.end();
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Erro ao buscar histórico estrangeiro' });
    }
});

app.post('/volumeDiaAnterior', async (req, res) => {
  const { sinal } = req.body;
  if (!sinal || !['Alta', 'Queda', 'Neutro'].includes(sinal)) {
    return res.status(400).json({ error: 'Campo sinal é obrigatório e deve ser Alta, Queda ou Neutro' });
  }

  const timestamp = new Date();

  try {
    const conn = await mysql.createConnection(dbConfig);

    await conn.execute(
      'INSERT INTO ativos (tipo, nome, sinal, variacao, timestamp, tipo_contagem) VALUES (?, ?, ?, ?, ?, ?)',
      ['seguranca', 'VolumeDiaAnt', sinal, 0, timestamp, 'simple']
    );

    await conn.end();
    res.json({ message: 'Volume do dia anterior inserido com sucesso', sinal });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erro ao inserir VolumeDiaAnterior' });
  }
});

// Função auxiliar para definir tipo do ativo
function definirTipoAtivo(nome) {
  return nome.includes('USD') ? 'seguranca' : 'risco';
}

const PORT = 3000;
app.listen(PORT, () => console.log(`API rodando na porta ${PORT}`));