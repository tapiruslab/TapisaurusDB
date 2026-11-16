<?php

namespace TapisaurusDB;

/**
 * 🔥 TapisaurusDB Official PHP SDK — The AI-Native NoSQL Database Client.
 */
class TapisaurusDB
{
    private string $endpoint;

    public function __construct(string $endpoint = 'http://localhost:27018')
    {
        $this->endpoint = rtrim($endpoint, '/');
    }

    public function collection(string $name): TapisaurusCollection
    {
        return new TapisaurusCollection($this, $name);
    }

    public function query(string $queryString): mixed
    {
        $res = $this->request('/v1/query', 'POST', ['query' => $queryString]);
        if (!($res['success'] ?? false)) {
            throw new \RuntimeException($res['error'] ?? 'Query execution failed');
        }
        $data = $res['data'] ?? null;
        if (is_array($data)) {
            foreach (['Documents', 'Count', 'Inserted', 'Updated', 'Deleted', 'Success'] as $key) {
                if (array_key_exists($key, $data)) {
                    return $data[$key];
                }
            }
        }
        return $data;
    }

    public function request(string $path, string $method = 'GET', ?array $body = null): array
    {
        $url = $this->endpoint . $path;
        $options = [
            'http' => [
                'header'  => "Content-Type: application/json\r\nAccept: application/json\r\n",
                'method'  => $method,
                'content' => $body ? json_encode($body) : null,
                'ignore_errors' => true,
            ]
        ];
        $context  = stream_context_create($options);
        $result = @file_get_contents($url, false, $context);
        if ($result === false) {
            throw new \RuntimeException("Failed to connect to TapisaurusDB at $url");
        }
        return json_decode($result, true) ?: [];
    }
}

class TapisaurusCollection
{
    private TapisaurusDB $client;
    public string $name;

    public function __construct(TapisaurusDB $client, string $name)
    {
        $this->client = $client;
        $this->name = $name;
    }

    public function insert(array $document): string
    {
        $res = $this->client->request("/v1/collections/{$this->name}/insert", 'POST', $document);
        if (!($res['success'] ?? false)) {
            throw new \RuntimeException($res['error'] ?? 'Insert failed');
        }
        return $res['data']['id'];
    }

    public function find(?array $filter = null): mixed
    {
        $filterStr = $filter ? json_encode($filter) : '{}';
        return $this->client->query("db.{$this->name}.find($filterStr)");
    }

    public function vectorSearch(array $vector, int $topK = 10): mixed
    {
        $vecJson = json_encode($vector);
        return $this->client->query("FIND {$this->name} VECTOR NEAR $vecJson TOP $topK");
    }
}
