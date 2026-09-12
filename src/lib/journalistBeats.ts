export type JournalistBeat = {
  slug: string;
  name: string;
  title: string;
  description: string;
  related: string[];
};

const curatedBeats: Record<string, Omit<JournalistBeat, "slug" | "name">> = {
  tech: {
    title: "Technology Journalists and Reporters",
    description: "Find technology journalists covering the companies, products and trends shaping the industry.",
    related: ["software", "computers", "iot", "crypto"],
  },
  software: {
    title: "Software Journalists and Reporters",
    description: "Browse journalists whose beat includes software, SaaS, developer tools and enterprise technology.",
    related: ["tech", "computers", "iot", "crypto"],
  },
  computers: {
    title: "Computer Journalists and Reporters",
    description: "Browse journalists covering computing, hardware, software and the businesses behind them.",
    related: ["tech", "software", "electronics", "smartphones"],
  },
  crypto: {
    title: "Crypto Journalists and Reporters",
    description: "Find journalists who cover cryptocurrency, blockchain and the financial technology ecosystem.",
    related: ["tech", "software", "business", "iot"],
  },
  iot: {
    title: "IoT Journalists and Reporters",
    description: "Browse journalists covering connected devices, industrial IoT and the technology around them.",
    related: ["tech", "electronics", "software", "smartphones"],
  },
  "mixed-reality": {
    title: "Mixed Reality Journalists and Reporters",
    description: "Find journalists covering AR, VR, spatial computing and mixed-reality products.",
    related: ["tech", "gaming", "electronics", "computers"],
  },
  smartphones: {
    title: "Smartphone Journalists and Reporters",
    description: "Browse journalists covering smartphones, mobile devices and consumer technology.",
    related: ["tech", "electronics", "computers", "audio"],
  },
  electronics: {
    title: "Electronics Journalists and Reporters",
    description: "Find journalists covering consumer electronics, devices and technology launches.",
    related: ["tech", "smartphones", "computers", "audio"],
  },
  audio: {
    title: "Audio Journalists and Reporters",
    description: "Browse journalists covering audio technology, headphones, speakers and sound culture.",
    related: ["electronics", "tech", "smartphones", "tv"],
  },
  cars: {
    title: "Automotive Journalists and Reporters",
    description: "Find automotive journalists covering vehicles, mobility, car launches and the future of transport.",
    related: ["tech", "electronics", "business", "travel"],
  },
  gaming: {
    title: "Gaming Journalists and Reporters",
    description: "Browse journalists covering video games, gaming culture, esports and interactive entertainment.",
    related: ["tech", "mixed-reality", "computers", "tv"],
  },
  tv: {
    title: "Television Journalists and Reporters",
    description: "Find journalists covering television, streaming, entertainment and the screen industry.",
    related: ["gaming", "audio", "fashion", "photography"],
  },
  fashion: {
    title: "Fashion Journalists and Reporters",
    description: "Browse journalists covering fashion, retail, style, luxury and the business of apparel.",
    related: ["design", "photography", "travel", "business"],
  },
  design: {
    title: "Design Journalists and Reporters",
    description: "Find journalists covering design, architecture, creative work and design-led brands.",
    related: ["fashion", "photography", "travel", "business"],
  },
  travel: {
    title: "Travel Journalists and Reporters",
    description: "Browse journalists covering destinations, hospitality, tourism and travel experiences.",
    related: ["photography", "fashion", "cars", "business"],
  },
  photography: {
    title: "Photography Journalists and Reporters",
    description: "Find journalists covering photography, cameras, visual culture and creative technology.",
    related: ["design", "travel", "fashion", "electronics"],
  },
  appliances: {
    title: "Home Appliance Journalists and Reporters",
    description: "Browse journalists covering home appliances, smart-home products and consumer living.",
    related: ["electronics", "iot", "design", "tech"],
  },
  business: {
    title: "Business Journalists and Reporters",
    description: "Find journalists covering companies, markets, leadership and business trends.",
    related: ["tech", "crypto", "cars", "fashion"],
  },
};

const titleCase = (value: string) => value.replace(/\b\w/g, (letter) => letter.toUpperCase());

export function journalistBeatFromRecord(record: { beat_slug: string; beat_key: string }): JournalistBeat {
  const curated = curatedBeats[record.beat_slug];
  const label = titleCase(record.beat_key);
  return {
    slug: record.beat_slug,
    name: record.beat_key,
    title: curated?.title ?? `${label} Journalists and Reporters`,
    description: curated?.description ?? `Browse journalists in Media AI's ${label.toLowerCase()} directory, with current outlets and coverage beats.`,
    related: curated?.related ?? [],
  };
}
