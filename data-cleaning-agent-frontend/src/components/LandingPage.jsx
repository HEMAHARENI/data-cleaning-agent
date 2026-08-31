import React, { useState, useEffect } from "react";
import { Sparkles, Upload, Zap, BarChart3, ArrowRight, CheckCircle } from "lucide-react";

// Add a wrapper for layout and a visually-engaging background
export default function LandingPage({ setStep }) {
  const [currentBenefit, setCurrentBenefit] = useState(0);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

  const benefits = [
    "Turn messy spreadsheets into clean datasets",
    "Remove duplicates and fix inconsistencies", 
    "Handle missing values intelligently",
    "Prepare data for machine learning models"
  ];

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentBenefit((prev) => (prev + 1) % benefits.length);
    }, 2500);
    return () => clearInterval(interval);
  }, []);

  const handleMouseMove = (e) => {
    setMousePosition({ x: e.clientX, y: e.clientY });
  };

  return (
    <div 
      className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 relative overflow-hidden"
      onMouseMove={handleMouseMove}
    >
      {/* Animated background orbs */}
      <div className="absolute inset-0 pointer-events-none">
        <div 
          className="absolute w-96 h-96 bg-gradient-to-r from-blue-400/20 to-purple-400/20 rounded-full blur-3xl animate-pulse"
          style={{
            left: mousePosition.x * 0.05 + 'px',
            top: mousePosition.y * 0.05 + 'px',
            transition: 'all 0.3s ease'
          }}
        />
        <div className="absolute top-1/4 right-1/4 w-64 h-64 bg-gradient-to-r from-pink-400/20 to-orange-400/20 rounded-full blur-2xl animate-bounce" style={{ animationDuration: '3s' }} />
        <div className="absolute bottom-1/4 left-1/4 w-80 h-80 bg-gradient-to-r from-green-400/20 to-blue-400/20 rounded-full blur-3xl animate-pulse" style={{ animationDelay: '1s' }} />
      </div>

      <div className="relative z-10 container mx-auto px-6 py-20">
        {/* Hero Section */}
        <div className="text-center mb-20">
          {/* Floating Icon */}
          <div className="inline-flex items-center justify-center w-24 h-24 bg-gradient-to-r from-blue-500 to-purple-600 rounded-full mb-8 shadow-2xl shadow-purple-500/50">
            <Sparkles className="w-12 h-12 text-white animate-spin" style={{ animationDuration: '3s' }} />
          </div>

          {/* Main Headline */}
          <h1 className="text-6xl font-black mb-6">
            <span className="bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 bg-clip-text text-transparent">
              Clean Data,
            </span>
            <br />
            <span className="text-white">
              Clear Mind
            </span>
          </h1>

          {/* Rotating Benefits */}
          <div className="h-16 flex items-center justify-center mb-8">
            <p className="text-xl text-gray-300 max-w-2xl transition-all duration-500">
              {benefits[currentBenefit]}
            </p>
          </div>

          {/* CTA Button */}
          <button 
            onClick={() => setStep(1)}
            className="group relative bg-gradient-to-r from-blue-500 to-purple-600 text-white px-10 py-4 rounded-full text-lg font-semibold shadow-2xl shadow-purple-500/50 hover:shadow-purple-500/70 transition-all duration-300 hover:scale-105"
          >
            <span className="flex items-center gap-3">
              Start Cleaning Your Data
              <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
            </span>
          </button>
        </div>

        {/* Features Grid */}
        <div className="grid md:grid-cols-3 gap-8 mb-20">
          <FeatureCard
            icon={<Upload className="w-8 h-8" />}
            title="Upload Anywhere"
            description="Drop your CSV, Excel, or JSON files and watch the magic happen. No complex setup required."
            gradient="from-green-400 to-blue-500"
          />
          <FeatureCard
            icon={<Zap className="w-8 h-8" />}
            title="Smart Processing"
            description="Our algorithms detect patterns and anomalies in your data, fixing issues you didn't even know existed."
            gradient="from-yellow-400 to-orange-500"
          />
          <FeatureCard
            icon={<BarChart3 className="w-8 h-8" />}
            title="ML Ready"
            description="Get perfectly formatted data that's ready for training your machine learning models immediately."
            gradient="from-pink-400 to-purple-500"
          />
        </div>

        {/* Social Proof */}
        <div className="text-center">
          <div className="flex justify-center items-center gap-6 mb-6">
            <div className="flex -space-x-2">
              {[1,2,3,4,5].map(i => (
                <div key={i} className={`w-10 h-10 rounded-full bg-gradient-to-r ${i % 2 === 0 ? 'from-blue-400 to-purple-500' : 'from-pink-400 to-orange-500'} border-2 border-white`} />
              ))}
            </div>
            <span className="text-gray-300">Join 2,847+ happy data scientists</span>
          </div>
          
          {/* Trust Indicators */}
          <div className="flex justify-center items-center gap-8 text-gray-400">
            <div className="flex items-center gap-2">
              <CheckCircle className="w-5 h-5 text-blue-400" />
              <span>No data stored</span>
            </div>
            <div className="flex items-center gap-2">
              <CheckCircle className="w-5 h-5 text-blue-400" />
              <span>Privacy first</span>
            </div>
            <div className="flex items-center gap-2">
              <CheckCircle className="w-5 h-5 text-blue-400" />
              <span>Lightning fast</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function FeatureCard({ icon, title, description, gradient }) {
  return (
    <div className={`group relative bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-8 hover:bg-white/10 transition-all duration-300 hover:scale-105`}>
      <div className={`inline-flex items-center justify-center w-16 h-16 bg-gradient-to-r ${gradient} rounded-xl mb-6 shadow-lg`}>
        <div className="text-white">
          {icon}
        </div>
      </div>
      
      <h3 className="text-xl font-bold text-white mb-4 group-hover:text-transparent group-hover:bg-gradient-to-r group-hover:from-blue-400 group-hover:to-purple-400 group-hover:bg-clip-text transition-all duration-300">
        {title}
      </h3>
      
      <p className="text-gray-300 leading-relaxed">
        {description}
      </p>
    </div>
  );
}