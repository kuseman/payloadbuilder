package org.kuse.payloadbuilder.editor;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.plaf.basic.BasicButtonUI;

/** View for tab header component */
class TabComponentView extends JPanel
{
    private final JLabel lblTitle;

    TabComponentView(String title, Icon icon)
    {
        this(title, icon, null);
    }

    TabComponentView(String title, Icon icon, final Runnable closeAction)
    {
        super(new FlowLayout(FlowLayout.LEFT, 0, 0));
        setOpaque(false);
        setBorder(BorderFactory.createEmptyBorder(2, 0, 0, 0));

        lblTitle = new JLabel(title, icon, SwingConstants.CENTER);
        //CSOFF
        lblTitle.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 5));
        //CSON
        add(lblTitle);

        if (closeAction != null)
        {
            JButton button = new TabButton();
            button.addActionListener(evt -> closeAction.run());
            add(button);
        }
    }

    public void setTitle(String titleString)
    {
        lblTitle.setText(titleString);
    }

    /** Tab button */
    class TabButton extends JButton
    {
        private static final int DELTA = 6;
        private static final int SIZE = 17;

        TabButton()
        {
            int size = SIZE;
            setPreferredSize(new Dimension(size, size));
            //Make the button looks the same for all Laf's
            setUI(new BasicButtonUI());
            //Make it transparent
            setContentAreaFilled(false);
            //No need to be focusable
            setFocusable(false);
            setBorder(BorderFactory.createEtchedBorder());
            setBorderPainted(false);
            //Making nice rollover effect
            //we use the same listener for all buttons
            addMouseListener(BUTTON_MOUSE_LISTENER);
            setRolloverEnabled(true);
        }

        //we don't want to update UI for this button
        @Override
        public void updateUI()
        {
        }

        //paint the cross
        @Override
        protected void paintComponent(Graphics g)
        {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            //shift the image for pressed buttons
            if (getModel().isPressed())
            {
                g2.translate(1, 1);
            }
            g2.setStroke(new BasicStroke(2));
            g2.setColor(Color.BLACK);
            if (getModel().isRollover())
            {
                g2.setColor(Color.MAGENTA);
            }
            int delta = DELTA;
            g2.drawLine(delta, delta, getWidth() - delta - 1, getHeight() - delta - 1);
            g2.drawLine(getWidth() - delta - 1, delta, delta, getHeight() - delta - 1);
            g2.dispose();
        }
    }

    //CSOFF
    private static final MouseListener BUTTON_MOUSE_LISTENER = new MouseAdapter()
    //CSON
    {
        @Override
        public void mouseEntered(MouseEvent e)
        {
            Component component = e.getComponent();
            if (component instanceof AbstractButton)
            {
                AbstractButton button = (AbstractButton) component;
                button.setBorderPainted(true);
            }
        }

        @Override
        public void mouseExited(MouseEvent e)
        {
            Component component = e.getComponent();
            if (component instanceof AbstractButton)
            {
                AbstractButton button = (AbstractButton) component;
                button.setBorderPainted(false);
            }
        }
    };
}
