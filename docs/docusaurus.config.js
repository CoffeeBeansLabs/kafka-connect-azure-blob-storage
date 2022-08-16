const path = require('path');
// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Kafka Connect Azure Blob Storage',
  tagline: 'Documentation',
  url: 'https://coffeebeanslabs.github.io',
  baseUrl: '/kafka-connect-azure-blob-storage/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/logo@2x.png',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'CoffeeBeansLabs', // Usually your GitHub org/user name.
  projectName: 'kafka-connect-azure-blob-storage', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/CoffeeBeansLabs/kafka-connect-azure-blob-storage',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/CoffeeBeansLabs/kafka-connect-azure-blob-storage',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        logo: {
          alt: 'CoffeeBeans Consulting Logo',
          src: 'img/logo@2x.png',
        },
        items: [
          {
            type: 'doc',
            docId: 'documentation',
            position: 'left',
            label: 'Docs',
          },
          {
            href: 'https://github.com/CoffeeBeansLabs/kafka-connect-azure-blob-storage',
            label: 'GitHub',
            position: 'right',
          },
          {
            type: 'docsVersionDropdown',
          },
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Documentation',
                to: '/docs/documentation',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/CoffeeBeansLabs/kafka-connect-azure-blob-storage',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} CoffeeBeans Consulting`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
