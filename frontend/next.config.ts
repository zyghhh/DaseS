import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  // BFF：将 /api/backend/* 反向代理到 FastAPI
  async rewrites() {
    return [
      {
        source: '/api/backend/:path*',
        destination: `${process.env.FASTAPI_URL ?? 'http://localhost:8000'}/:path*`,
      },
    ]
  },
}

export default nextConfig
